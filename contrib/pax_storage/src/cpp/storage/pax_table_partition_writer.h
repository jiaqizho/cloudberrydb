#pragma once
#include <vector>

#include "storage/micro_partition.h"
#include "storage/pax.h"

namespace pax {
class PartitionObject;
class TableParitionWriter : public TableWriter {
 public:
  explicit TableParitionWriter(Relation relation, std::unique_ptr<PartitionObject> &&part_obj);

  ~TableParitionWriter() override;

  void WriteTuple(TupleTableSlot *slot) override;

  void Open() override;

  void Close() override;

#ifndef RUN_GTEST
 private:
#endif
  std::vector<std::vector<size_t>> GetPartitionMergeInfos();

 private:
  std::unique_ptr<PartitionObject> part_obj_;
  std::vector<std::unique_ptr<MicroPartitionWriter>> writers_;
  std::vector<std::shared_ptr<MicroPartitionStats>> mp_stats_array_;
  std::vector<size_t> num_tuples_;
  std::vector<BlockNumber> current_blocknos_;

  int writer_counts_;
};

}  // namespace pax
