#pragma once
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecNoHdrColumn final : public PaxVecNonFixedEncodingColumn {
 public:
  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxEncoder::EncodingOption &encoder_options)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     encoder_options) {}

  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxDecoder::DecodingOption &decoding_option)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     decoding_option) {}

  Datum GetDatum(size_t position) override {
    CBDB_CHECK(position < offsets_->GetSize(),
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                   position, GetRows(), DebugString().c_str()));
    // This situation happend when writing
    // The `offsets_` have not fill the last one
    if (unlikely(position == offsets_->GetSize() - 1)) {
      if (null_bitmap_ && null_bitmap_->Test(position)) {
        return PointerGetDatum(nullptr);
      }
      return PointerGetDatum(data_->GetBuffer() + (*offsets_)[position]);
    }

    auto start_offset = (*offsets_)[position];
    auto last_offset = (*offsets_)[position + 1];

    if (start_offset == last_offset) {
      return PointerGetDatum(nullptr);
    }

    return PointerGetDatum(data_->GetBuffer() + start_offset);
  }

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeVecNoHeader;
  }
};

}  // namespace pax
