#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionRowFilterReader : public MicroPartitionReaderProxy {
 public:
  static std::unique_ptr<MicroPartitionReader> New(
      std::unique_ptr<MicroPartitionReader> &&reader,
      std::shared_ptr<PaxFilter> filter,
      std::shared_ptr<Bitmap8> visibility_bitmap = nullptr);

  MicroPartitionRowFilterReader() = default;
  ~MicroPartitionRowFilterReader() override = default;
  bool ReadTuple(TupleTableSlot *slot) override;

 private:
  inline void SetVisibilityBitmap(std::shared_ptr<Bitmap8> visibility_bitmap) {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

  std::shared_ptr<MicroPartitionReader::Group> GetNextGroup(TupleDesc desc);
  bool TestRowScanInternal(TupleTableSlot *slot, ExprState *estate,
                           AttrNumber attno);

  // filter is referenced only, the reader doesn't own it.
  std::shared_ptr<PaxFilter> filter_;
  std::shared_ptr<MicroPartitionReader::Group> group_;
  size_t current_group_row_index_ = 0;
  size_t group_index_ = 0;
  // only referenced
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_;
};
}  // namespace pax
