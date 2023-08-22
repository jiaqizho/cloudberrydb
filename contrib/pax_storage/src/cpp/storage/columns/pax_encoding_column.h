#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

template <typename T>
class PaxEncodingColumn : public PaxCommColumn<T> {
 public:
  PaxEncodingColumn(uint64 capacity,
                    const PaxEncoder::EncodingOption &encoding_option);

  PaxEncodingColumn(uint64 capacity,
                    const PaxDecoder::DecodingOption &decoding_option);

  ~PaxEncodingColumn() override;

  void Set(DataBuffer<T> *data) override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t GetNonNullRows() const override;

  int64 GetOriginLength() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  virtual ColumnEncoding_Kind GetDefaultColumnType() = 0;

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxEncoder *encoder_;
  uint64 origin_len_;
  uint64 non_null_rows_;

  PaxDecoder::DecodingOption decoder_options_;
  PaxDecoder *decoder_;
  DataBuffer<char> *shared_data_;

  PaxCompressor *compressor_;
  bool compress_route_;
};

extern template class PaxEncodingColumn<int8>;
extern template class PaxEncodingColumn<int16>;
extern template class PaxEncodingColumn<int32>;
extern template class PaxEncodingColumn<int64>;

}  // namespace pax
