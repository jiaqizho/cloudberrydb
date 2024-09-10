#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {
class PaxNonFixedEncodingColumn : public PaxNonFixedColumn {
 public:
  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 lengths_capacity,
                            const PaxEncoder::EncodingOption &encoder_options);

  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 lengths_capacity,
                            const PaxDecoder::DecodingOption &decoding_option);

  ~PaxNonFixedEncodingColumn() override;

  void Set(std::shared_ptr<DataBuffer<char>> data, std::shared_ptr<DataBuffer<int32>> lengths,
           size_t total_size) override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetLengthBuffer() override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

#ifdef BUILD_RB_RET_DICT
  inline std::shared_ptr<DataBuffer<char>> GetUndecodedBuffer() { return shared_data_; }
#endif

  // The reason why `PaxNonFixedEncodingColumn` not override the
  // method `GetRangeBuffer` and `GetNonNullRows` is that
  // `PaxNonFixedEncodingColumn` don't have any streaming encoding, also
  // `shared_data_` will own the same buffer with `PaxNonFixedColumn::data_`.

 protected:
  void InitEncoder();
  void InitLengthStreamCompressor();
  void InitDecoder();
  void InitLengthStreamDecompressor();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  std::shared_ptr<PaxEncoder> encoder_;
  std::shared_ptr<PaxDecoder> decoder_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> lengths_compressor_;
  std::shared_ptr<DataBuffer<char>> shared_lengths_data_;
};

}  // namespace pax
