#pragma once
#include "storage/micro_partition.h"

#ifdef VEC_BUILD

namespace pax {

class VecAdapter;

class PaxVecReader : public MicroPartitionReader {
 public:
  // If enable read tuple from vec reader,
  // then OrcReader will be hold by PaxVecReader,
  // current MicroPartitionReader lifecycle will be bound to the PaxVecReader)
  PaxVecReader(MicroPartitionReader *reader, VecAdapter *adapter);

  ~PaxVecReader() override;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(CTupleSlot *cslot) override;

  size_t GetGroupNums() override;

  MicroPartitionReader::Group *ReadGroup(size_t index) override;

 private:
  MicroPartitionReader *reader_;
  VecAdapter *adapter_;

  MicroPartitionReader::Group *working_group_;
  size_t current_group_index_;
};

}  // namespace pax

#endif  // VEC_BUILD
