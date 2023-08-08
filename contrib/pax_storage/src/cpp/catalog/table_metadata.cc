#include "catalog/table_metadata.h"

#include <utility>

#include "exceptions/CException.h"

namespace pax {
TableMetadata *TableMetadata::Create(const Relation parent_relation,
                                     const Snapshot snapshot) {
  return new TableMetadata(parent_relation, snapshot);
}

std::unique_ptr<TableMetadata::Iterator> TableMetadata::NewIterator() {
  std::vector<MicroPartitionMetadata> micro_partitions;
  GetAllMicroPartitionMetadata(micro_partitions);

  return std::unique_ptr<Iterator>(
      Iterator::Create(std::move(micro_partitions)));
}

TableMetadata::TableMetadata(const Relation parent_relation,
                             const Snapshot snapshot)
    : parent_relation_(parent_relation), snapshot_(snapshot) {}

void TableMetadata::GetAllMicroPartitionMetadata(
    std::vector<pax::MicroPartitionMetadata> &micro_partitions) {
  cbdb::GetAllMicroPartitionMetadata(parent_relation_, snapshot_,
                                     micro_partitions);
}

std::unique_ptr<TableMetadata::Iterator> TableMetadata::Iterator::Create(
    std::vector<pax::MicroPartitionMetadata> &&micro_partitions) {
  return std::unique_ptr<TableMetadata::Iterator>(
      new Iterator(micro_partitions));
}
void TableMetadata::Iterator::Init() {}

std::size_t TableMetadata::Iterator::Index() const { return current_index_; }

bool TableMetadata::Iterator::HasNext() const {
  return current_index_ + 1 < micro_partitions_.size();
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
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
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

pax::MicroPartitionMetadata TableMetadata::Iterator::Next() {
  Assert(current_index_ >= 0 && current_index_ + 1 < micro_partitions_.size());
  return micro_partitions_[++current_index_];
}

pax::MicroPartitionMetadata TableMetadata::Iterator::Current() const {
  return micro_partitions_[current_index_];
}

TableMetadata::Iterator::~Iterator() { micro_partitions_.clear(); }

TableMetadata::Iterator::Iterator(
    std::vector<pax::MicroPartitionMetadata>
        &micro_partitions)  // NOLINT(runtime/references)
    : current_index_(0), micro_partitions_(micro_partitions) {}

}  // namespace pax
