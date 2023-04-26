#include "catalog/table_metadata.h"
#include <assert.h>
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
  assert(offset >= 0 - max_mpartition_offset && offset <= max_mpartition_offset);
  switch (whence) {
    case ITER_SEEK_POS_BEGIN:
      current_idx = offset;
      break;
    case ITER_SEEK_POS_CUR:
      current_idx += offset;
      break;
    case ITER_SEEK_POS_END:
      current_idx = max_mpartition_offset - offset;
      break;
    default:
      ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT),
         errmsg("TableMetadata Iterator seek error, no such kind micro partition seek type: %d.", whence)));
  }
  // TODO(Tony) : Not sure the error handling when current_index_ exceeds micropartition file size range case
  // which should be handled, temporary solution is to reset current_index_ to its min/max bound after seeking.
  if (current_idx > max_mpartition_offset)
    current_index_ = max_mpartition_offset;
  else if (current_idx < 0)
    current_index_ = 0;
  else
    current_index_ = current_idx;
}
}  // namespace pax
