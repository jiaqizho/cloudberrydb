
#include "storage/columns/pax_column_int.h"

namespace pax {

template <typename T>
PaxIntColumn<T>::PaxIntColumn(const PaxEncoder::EncodingOption &encoding_option)
    : PaxEncodingColumn<T>(DEFAULT_CAPACITY, encoding_option) {
  PaxEncodingColumn<T>::InitEncoder();
}

template <typename T>
PaxIntColumn<T>::PaxIntColumn(uint64 capacity,
                              const PaxEncoder::EncodingOption &encoding_option)
    : PaxEncodingColumn<T>(capacity, encoding_option) {
  PaxEncodingColumn<T>::InitEncoder();
}

template <typename T>
PaxIntColumn<T>::PaxIntColumn(const PaxDecoder::DecodingOption &decoding_option)
    : PaxEncodingColumn<T>(DEFAULT_CAPACITY, decoding_option) {
  PaxEncodingColumn<T>::InitDecoder();
}

template <typename T>
PaxIntColumn<T>::PaxIntColumn(uint64 capacity,
                              const PaxDecoder::DecodingOption &decoding_option)
    : PaxEncodingColumn<T>(capacity, decoding_option) {
  PaxEncodingColumn<T>::InitDecoder();
}

template <typename T>
ColumnEncoding_Kind PaxIntColumn<T>::GetDefaultColumnType() {
  return sizeof(T) >= 4 ? ColumnEncoding_Kind::ColumnEncoding_Kind_ORC_RLE_V2
                        : ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
}

template class PaxIntColumn<int8>;
template class PaxIntColumn<int16>;
template class PaxIntColumn<int32>;
template class PaxIntColumn<int64>;

}  // namespace pax
