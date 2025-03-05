/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * pax_encoding_non_fixed_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_non_fixed_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {

void PaxNonFixedEncodingColumn::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
    encoder_options_.compress_level = 5;
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_, true);
  if (encoder_) {
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (compressor_) {
    return;
  }

  PaxColumn::SetEncodeType(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
  PaxColumn::SetCompressLevel(0);
}

void PaxNonFixedEncodingColumn::InitLengthStreamCompressor() {
  Assert(encoder_options_.lengths_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  lengths_compressor_ = PaxCompressor::CreateBlockCompressor(
      encoder_options_.lengths_encode_type);
  SetLengthsEncodeType(encoder_options_.lengths_encode_type);
  SetLengthsCompressLevel(encoder_options_.lengths_compress_level);
}

void PaxNonFixedEncodingColumn::InitLengthStreamDecompressor() {
  Assert(decoder_options_.lengths_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  lengths_compressor_ = PaxCompressor::CreateBlockCompressor(
      decoder_options_.lengths_encode_type);
  SetLengthsEncodeType(decoder_options_.lengths_encode_type);
  SetLengthsCompressLevel(decoder_options_.lengths_compress_level);
}

void PaxNonFixedEncodingColumn::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);

  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<int8>(decoder_options_);
  if (decoder_) {
    shared_data_ = std::make_shared<DataBuffer<char>>(*PaxNonFixedColumn::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 lengths_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(data_capacity, lengths_capacity),
      encoder_options_(encoder_options),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr),
      lengths_compressor_(nullptr),
      shared_lengths_data_(nullptr) {
  InitEncoder();
  InitLengthStreamCompressor();
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 lengths_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(data_capacity, lengths_capacity),
      decoder_options_(decoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr),
      lengths_compressor_(nullptr),
      shared_lengths_data_(nullptr) {
  InitDecoder();
  InitLengthStreamDecompressor();
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() { }

void PaxNonFixedEncodingColumn::Set(std::shared_ptr<DataBuffer<char>> data,
                                    std::shared_ptr<DataBuffer<int32>> lengths,
                                    size_t total_size) {
  bool exist_decoder;
  Assert(data && lengths);

  auto data_decompress = [&]() {
    Assert(!compress_route_);
    Assert(bool(compressor_) != bool(decoder_));

    if (data->Used() == 0) {
      return;
    }

    if (compressor_) {
      auto d_size = compressor_->Decompress(
          PaxNonFixedColumn::data_->Start(),
          PaxNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::data_->Brush(d_size);
    }

    if (decoder_) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();

      // `data_` have the same buffer with `shared_data_`
      PaxNonFixedColumn::data_->Brush(shared_data_->Used());
      // no delete the origin data
      shared_data_ = data;
    }
  };

  auto lengths_decompress = [&]() {
    Assert(!compress_route_);
    Assert(lengths_compressor_);

    if (lengths->Used() != 0) {
      auto d_size = lengths_compressor_->Decompress(
          PaxNonFixedColumn::lengths_->Start(),
          PaxNonFixedColumn::lengths_->Capacity(), lengths->Start(),
          lengths->Used());
      if (lengths_compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::lengths_->Brush(d_size);
    }

    BuildOffsets();
  };

  exist_decoder = compressor_ || decoder_;

  if (exist_decoder && lengths_compressor_) {
    data_decompress();
    lengths_decompress();

    estimated_size_ = total_size;
  } else if (exist_decoder && !lengths_compressor_) {
    data_decompress();

    lengths_ = lengths;
    BuildOffsets();

    estimated_size_ = total_size;
  } else if (!exist_decoder && lengths_compressor_) {
    data_ = data;

    lengths_decompress();

    estimated_size_ = total_size;
  } else {  // (!compressor_ && !lengths_compressor_)
    PaxNonFixedColumn::Set(data, lengths, total_size);
  }
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer() {
  bool exist_encoder;
  exist_encoder = compressor_ || encoder_;

  if (exist_encoder && compress_route_) {
    Assert(!compressor_ || !encoder_);

    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (PaxNonFixedColumn::data_->Used() == 0) {
      return PaxNonFixedColumn::GetBuffer();
    }

    // do compressed
    if (compressor_) {
      size_t bound_size =
          compressor_->GetCompressBound(PaxNonFixedColumn::data_->Used());
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      auto c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxNonFixedColumn::data_->Start(), PaxNonFixedColumn::data_->Used(),
          encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (encoder_) {
      shared_data_ =
          std::make_shared<DataBuffer<char>>(PaxNonFixedColumn::data_->Used());
      encoder_->SetDataBuffer(shared_data_);

      char *data_buffer = PaxNonFixedColumn::data_->GetBuffer();
      for (size_t i = 0; i < lengths_->GetSize(); i++) {
        encoder_->Append(data_buffer, (*lengths_)[i]);
        data_buffer += (*lengths_)[i];
      }

      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetBuffer();
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetLengthBuffer() {
  if (lengths_compressor_ && compress_route_) {
    if (shared_lengths_data_) {
      return std::make_pair(shared_lengths_data_->Start(),
                            shared_lengths_data_->Used());
    }

    if (PaxNonFixedColumn::lengths_->Used() == 0) {
      return PaxNonFixedColumn::GetLengthBuffer();
    }

    size_t bound_size = lengths_compressor_->GetCompressBound(
        PaxNonFixedColumn::lengths_->Used());
    shared_lengths_data_ = std::make_shared<DataBuffer<char>>(bound_size);

    auto c_size = lengths_compressor_->Compress(
        shared_lengths_data_->Start(), shared_lengths_data_->Capacity(),
        PaxNonFixedColumn::lengths_->Start(),
        PaxNonFixedColumn::lengths_->Used(), encoder_options_.compress_level);

    if (lengths_compressor_->IsError(c_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
    }

    shared_lengths_data_->Brush(c_size);
    return std::make_pair(shared_lengths_data_->Start(),
                          shared_lengths_data_->Used());
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetLengthBuffer();
}

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return PaxNonFixedColumn::data_->Used();
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax
