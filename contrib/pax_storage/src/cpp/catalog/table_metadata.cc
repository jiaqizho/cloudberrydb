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

bool TableMetadata::Iterator::HasNext() const {
  return current_index_ < micro_partitions_.size();
}

void TableMetadata::Iterator::Rewind() {
  current_index_ = 0;
}

pax::MicroPartitionMetadata TableMetadata::Iterator::Next() {
  Assert(current_index_ >= 0 && current_index_ < micro_partitions_.size());
  CBDB_CHECK(current_index_ < micro_partitions_.size(), cbdb::CException::kExTypeOutOfRange);
  return micro_partitions_[current_index_++];
}

TableMetadata::Iterator::~Iterator() { micro_partitions_.clear(); }

TableMetadata::Iterator::Iterator(
    std::vector<pax::MicroPartitionMetadata>
        &micro_partitions)  // NOLINT(runtime/references)
    : current_index_(0), micro_partitions_(micro_partitions) {}

}  // namespace pax
