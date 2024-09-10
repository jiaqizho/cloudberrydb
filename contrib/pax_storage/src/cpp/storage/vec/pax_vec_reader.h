#pragma once
#include "storage/micro_partition.h"
#include "storage/vec/arrow_wrapper.h"

#ifdef VEC_BUILD

namespace pax {

class PaxFilter;
class VecAdapter;
class PaxFragmentInterface;

class PaxVecReader : public MicroPartitionReaderProxy {
 public:
  // If enable read tuple from vec reader,
  // then OrcReader will be hold by PaxVecReader,
  // current MicroPartitionReader lifecycle will be bound to the PaxVecReader)
  PaxVecReader(std::unique_ptr<MicroPartitionReader> &&reader,
               std::shared_ptr<VecAdapter> adapter,
               std::shared_ptr<PaxFilter> filter);

  ~PaxVecReader() override;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  size_t GetTupleCountsInGroup(size_t group_index) override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

  std::unique_ptr<MicroPartitionReader::Group> ReadGroup(size_t index) override;

  std::shared_ptr<arrow::RecordBatch> ReadBatch();

 private:
  std::shared_ptr<VecAdapter> adapter_;

  std::unique_ptr<MicroPartitionReader::Group> working_group_;
  size_t current_group_index_;
  std::shared_ptr<PaxFilter> filter_;

};

}  // namespace pax

#endif  // VEC_BUILD
