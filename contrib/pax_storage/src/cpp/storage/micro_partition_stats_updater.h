#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionStatsUpdater {
 public:
  // visibility_bitmap
  // v1: 111000
  // v2: 000100(updater) -> 111100(reader)
  MicroPartitionStatsUpdater(MicroPartitionReader *reader,
                             std::shared_ptr<Bitmap8> visibility_bitmap);
  std::shared_ptr<MicroPartitionStats> Update(
      TupleTableSlot *slot, const std::vector<int> &minmax_columns,
      const std::vector<int> &bf_columns);

 private:
  MicroPartitionReader *reader_;
  std::vector<bool> exist_invisible_tuples_;
};
}  // namespace pax
