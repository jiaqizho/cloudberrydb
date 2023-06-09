#include "catalog/table_metadata.h"

#include <utility>

#include "exceptions/CException.h"

namespace pax {
std::unique_ptr<TableMetadata::Iterator> TableMetadata::NewIterator() {
  std::vector<MicroPartitionMetadata> micro_partitions;
  getAllMicroPartitionMetadata(micro_partitions);

  return std::unique_ptr<Iterator>(
      Iterator::Create(std::move(micro_partitions)));
}

size_t TableMetadata::Iterator::Seek(int offset, IteratorSeekPosType whence) {
  std::size_t mpsize = micro_partitions_.size();
  int max_mpartition_offset = static_cast<int>(mpsize) - 1;
  int current_idx = current_index_;
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

  return current_index_;
}

}  // namespace pax
