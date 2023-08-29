#include "storage/columns/pax_encoding_column.h"

#include "storage/proto/proto_wrappers.h"
namespace pax {

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint64 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_options_(encoding_option),
      encoder_(nullptr),
      origin_len_(NO_ENCODE_ORIGIN_LEN),
      non_null_rows_(0),
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(true) {}

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint64 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_(nullptr),
      origin_len_(NO_ENCODE_ORIGIN_LEN),
      non_null_rows_(0),
      decoder_options_{decoding_option},
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(false) {}

template <typename T>
PaxEncodingColumn<T>::~PaxEncodingColumn() {
  delete encoder_;
  delete decoder_;
  delete shared_data_;
  delete compressor_;
}

template <typename T>
void PaxEncodingColumn<T>::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = GetDefaultColumnType();
  }

  PaxColumn::encoded_type_ = encoder_options_.column_encode_type;

  // Create a streaming encoder
  // If current `encoded_type_` can not create a streaming encoder,
  // `CreateStreamingEncoder` will return a nullptr. This may be
  // caused by three scenarios:
  //   - `encoded_type_` is not a encoding type.
  //   - `encoded_type_` is a encoding type, but not support it yet.
  //   - `encoded_type_` is no_encoding type.
  //
  // Not allow pass `default`type` of `encoded_type_` into
  // `CreateStreamingEncoder`, caller should change it before create a encoder.
  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_);

  if (encoder_) {
    origin_len_ = 0;
    // The memory owner change to `shared_data_`
    // Because PaxEncodingColumn can not predict when to resize the memory.
    // Should allow call memory resize in the encoding.
    PaxCommColumn<T>::data_->SetMemTakeOver(false);
    shared_data_ = new DataBuffer<char>(*PaxCommColumn<T>::data_);
    shared_data_->SetMemTakeOver(true);

    encoder_->SetDataBuffer(shared_data_);
  } else {
    // Create a block compressor
    // Compressor have a different interface with pax encoder
    // If no pax encoder no provided, then try to create a compressor.
    compressor_ =
        PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);

    // can't find any encoder or compressor
    // then should reset encode type
    // or will got origin length is -1 but still have encode type
    if (!compressor_) {
      PaxColumn::encoded_type_ =
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
    }
  }
}

template <typename T>
void PaxEncodingColumn<T>::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::encoded_type_ = decoder_options_.column_encode_type;

  decoder_ = PaxDecoder::CreateDecoder<T>(decoder_options_);
  if (decoder_) {
    shared_data_ = new DataBuffer<char>(*PaxCommColumn<T>::data_);
    decoder_->SetDataBuffer(shared_data_);
    // still need set source data buffer in `Set`
  } else {
    compressor_ =
        PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
    if (compressor_) {
      PaxCommColumn<T>::data_->SetMemTakeOver(false);
      shared_data_ = new DataBuffer<char>(*PaxCommColumn<T>::data_);
      shared_data_->SetMemTakeOver(true);
    }
  }
}

template <typename T>
void PaxEncodingColumn<T>::Set(DataBuffer<T> *data) {
  if (decoder_) {
    // should not decoding null
    if (data->Used() != 0) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      // should not setting null bitmap until vec version
      decoder_->Decoding(nullptr, 0);
    }

    Assert(!data->IsMemTakeOver());
    delete data;
  } else if (compressor_) {
    if (data->Used() != 0) {
      Assert(shared_data_);
      size_t d_size = compressor_->Decompress(shared_data_->Start(),
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
    PaxCommColumn<T>::data_->Reset();
    PaxCommColumn<T>::data_->Set(shared_data_->Start(),
                                 shared_data_->Capacity(), 0);
    PaxCommColumn<T>::data_->Brush(shared_data_->Used());

    Assert(!data->IsMemTakeOver());
    delete data;
  } else {
    PaxCommColumn<T>::Set(data);
  }
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetBuffer(size_t position) {
  CBDB_CHECK(!encoder_, cbdb::CException::ExType::kExTypeLogicError);

  if (decoder_) {
    Assert(shared_data_);
    CBDB_CHECK(position < shared_data_->Used() / sizeof(T),
               cbdb::CException::ExType::kExTypeOutOfRange);

    return std::make_pair(shared_data_->Start() + (sizeof(T) * position),
                          sizeof(T));
  }
  return PaxCommColumn<T>::GetBuffer(position);
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetBuffer() {
  if (encoder_) {
    encoder_->Flush();
  }

  if (shared_data_) {
    return std::make_pair(shared_data_->Start(), shared_data_->Used());
  } else if (compressor_ && !shared_data_ && compress_route_) {
    // all null field should not compress
    if (PaxCommColumn<T>::data_->Used() == 0) {
      return PaxCommColumn<T>::GetBuffer();
    } else {
      size_t bound_size =
          compressor_->GetCompressBound(PaxCommColumn<T>::data_->Used());
      shared_data_ = new DataBuffer<char>(bound_size);

      size_t c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxCommColumn<T>::data_->Start(), PaxCommColumn<T>::data_->Used(),
          encoder_options_.compress_lvl);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(c_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }
  } else {
    return PaxCommColumn<T>::GetBuffer();
  }

  // unreach
  Assert(false);
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetRangeBuffer(size_t start_pos,
                                                               size_t len) {
  CBDB_CHECK(!encoder_, cbdb::CException::ExType::kExTypeLogicError);

  if (decoder_) {
    Assert(shared_data_);
    CBDB_CHECK((start_pos + len) <= GetNonNullRows(),
               cbdb::CException::ExType::kExTypeOutOfRange);
    return std::make_pair(shared_data_->Start() + (sizeof(T) * start_pos),
                          sizeof(T) * len);
  }

  return PaxCommColumn<T>::GetRangeBuffer(start_pos, len);
}

template <typename T>
void PaxEncodingColumn<T>::Append(char *buffer, size_t size) {
  Assert(size == sizeof(T));
  if (encoder_) {
    // Should not call `PaxCommColumn::Append`,
    // but still need call `PaxColumn::Append` to push null bitmap.
    PaxColumn::Append(buffer, size);  // NOLINT

    non_null_rows_++;
    origin_len_ += size;
    encoder_->Append(*reinterpret_cast<T *>(buffer));
    if (shared_data_->Capacity() != PaxCommColumn<T>::capacity_) {
      PaxCommColumn<T>::capacity_ = shared_data_->Capacity();
    }
    return;
  }

  PaxCommColumn<T>::Append(buffer, size);
}

template <typename T>
int64 PaxEncodingColumn<T>::GetOriginLength() const {
  return compressor_ ? PaxCommColumn<T>::data_->Used() : origin_len_;
}

template <typename T>
size_t PaxEncodingColumn<T>::GetNonNullRows() const {
  if (decoder_) {
    // must be decoded
    Assert(shared_data_);
    return shared_data_->Used() / sizeof(T);
  }

  if (encoder_) {
    return non_null_rows_;
  }

  return PaxCommColumn<T>::GetNonNullRows();
}

template <typename T>
size_t PaxEncodingColumn<T>::PhysicalSize() const {
  if (shared_data_) {
    return shared_data_->Used();
  }

  return PaxCommColumn<T>::PhysicalSize();
}

template class PaxEncodingColumn<int8>;
template class PaxEncodingColumn<int16>;
template class PaxEncodingColumn<int32>;
template class PaxEncodingColumn<int64>;

}  // namespace pax
