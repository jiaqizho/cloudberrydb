
#pragma once
#include "storage/columns/pax_encoding_column.h"

namespace pax {

template <typename T>
class PaxIntColumn final : public PaxEncodingColumn<T> {
 public:
  explicit PaxIntColumn(const PaxEncoder::EncodingOption &encoding_option);

  PaxIntColumn(uint64 capacity,
               const PaxEncoder::EncodingOption &encoding_option);

  explicit PaxIntColumn(const PaxDecoder::DecodingOption &decoding_option);

  PaxIntColumn(uint64 capacity,
               const PaxDecoder::DecodingOption &decoding_option);

  ~PaxIntColumn() override = default;

 protected:
  ColumnEncoding_Kind GetDefaultColumnType() override;
};

extern template class PaxIntColumn<int8>;
extern template class PaxIntColumn<int16>;
extern template class PaxIntColumn<int32>;
extern template class PaxIntColumn<int64>;

}  // namespace pax
