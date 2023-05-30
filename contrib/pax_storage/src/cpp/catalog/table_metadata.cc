#include "catalog/table_metadata.h"

#include "exceptions/CException.h"

namespace pax {
std::shared_ptr<TableMetadata::Iterator> TableMetadata::NewIterator() {
  auto micro_partitions =
      std::make_shared<std::vector<std::shared_ptr<MicroPartitionMetadata>>>();

  getAllMicroPartitionMetadata(micro_partitions);

  return std::shared_ptr<Iterator>(Iterator::Create(micro_partitions));
}

void TableMetadata::Iterator::Seek(int offset, IteratorSeekPosType whence) {
  std::size_t mpsize = micro_partitions_->size();
  int max_mpartition_offset = static_cast<int>(mpsize) - 1;
  int current_idx = current_index_;
  Assert(offset >= 0 - max_mpartition_offset &&
         offset <= max_mpartition_offset);
  switch (whence) {
    case BEGIN:
      current_idx = offset;
      break;
    case CURRENT:
      current_idx += offset;
      break;
    case END:
      current_idx = max_mpartition_offset - offset;
      break;
    default:
      elog(WARNING,
           "TableMetadata Iterator seek error, no such "
           "kind micro partition seek type: %d.",
           whence);
      CBDB_RAISE(cbdb::CException::ExType::ExTypeLogicError);
  }
  // TODO(Tony) : Not sure the error handling when current_index_ exceeds
  // micropartition file size range case which should be handled, temporary
  // solution is to reset current_index_ to its min/max bound after seeking.
  if (current_idx > max_mpartition_offset)
    current_index_ = max_mpartition_offset;
  else if (current_idx < 0)
    current_index_ = 0;
  else
    current_index_ = current_idx;
}

}  // namespace pax
