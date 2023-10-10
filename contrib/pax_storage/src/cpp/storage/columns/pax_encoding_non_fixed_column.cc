#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "storage/pax_defined.h"

namespace pax {

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint64 capacity, const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(capacity),
      encoder_options_(encoder_options),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr) {
  if (encoder_options.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
  }

  PaxColumn::encoded_type_ = encoder_options_.column_encode_type;
  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
  if (!compressor_) {
    PaxColumn::encoded_type_ =
        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  }
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint64 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(capacity),
      decoder_options_(decoding_option),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr) {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::encoded_type_ = decoder_options_.column_encode_type;
  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
  if (compressor_) {
    PaxNonFixedColumn::data_->SetMemTakeOver(false);
    shared_data_ = new DataBuffer<char>(*PaxNonFixedColumn::data_);
    shared_data_->SetMemTakeOver(true);
  }
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() {
  delete compressor_;
  delete shared_data_;
}

void PaxNonFixedEncodingColumn::Set(DataBuffer<char> *data,
                                    DataBuffer<int64> *lengths,
                                    size_t total_size) {
  if (compressor_) {
    Assert(shared_data_);

    // still need update origin logic
    if (lengths_) {
      delete lengths_;
    }

    estimated_size_ = total_size;
    lengths_ = lengths;
    offsets_.clear();
    for (size_t i = 0; i < lengths_->GetSize(); i++) {
      offsets_.emplace_back(i == 0 ? 0 : offsets_[i - 1] + (*lengths_)[i - 1]);
    }

    if (data->Used() != 0) {
      auto d_size = compressor_->Decompress(shared_data_->Start(),
                                            shared_data_->Capacity(),
                                            data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }
      shared_data_->Brush(d_size);
    }

    // FIXME(jiaqizho): DataBuffer copy should change to ptr copy
    // Then we don't need update back `data_`
    PaxNonFixedColumn::data_->Reset();
    PaxNonFixedColumn::data_->Set(shared_data_->Start(),
                                  shared_data_->Capacity(), 0);
    PaxNonFixedColumn::data_->Brush(shared_data_->Used());

    Assert(!data->IsMemTakeOver());
    delete data;
  } else {
    PaxNonFixedColumn::Set(data, lengths, total_size);
  }
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer() {
  if (shared_data_) {
    return std::make_pair(shared_data_->Start(), shared_data_->Used());
  } else if (compressor_ && !shared_data_ && compress_route_) {
    if (PaxNonFixedColumn::data_->Used() == 0) {
      return PaxNonFixedColumn::GetBuffer();
    } else {
      size_t bound_size =
          compressor_->GetCompressBound(PaxNonFixedColumn::data_->Used());
      shared_data_ = new DataBuffer<char>(bound_size);

      auto c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxNonFixedColumn::data_->Start(), PaxNonFixedColumn::data_->Used(),
          encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }
      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }
  } else {
    return PaxNonFixedColumn::GetBuffer();
  }

  // unreach
  Assert(false);
}

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return compressor_ ? PaxNonFixedColumn::data_->Used() : NO_ENCODE_ORIGIN_LEN;
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax
