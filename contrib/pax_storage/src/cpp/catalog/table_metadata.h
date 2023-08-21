#pragma once

#include "comm/cbdb_api.h"

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "catalog/pax_aux_table.h"
#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class TableMetadata {
 public:
  class Iterator;

  static TableMetadata *Create(Relation parent_relation, Snapshot snapshot);

  std::unique_ptr<Iterator> NewIterator();

 private:
  TableMetadata(Relation parent_relation, Snapshot snapshot);

  void GetAllMicroPartitionMetadata(
      std::vector<pax::MicroPartitionMetadata>
          &micro_partitions);  // NOLINT (runtime/references)

 private:
  const Relation parent_relation_;
  const Snapshot snapshot_;
};  // class TableMetadata

// TODO(gongxun): enhance this iterator to support lazy loading
class TableMetadata::Iterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  static std::unique_ptr<Iterator> Create(
      std::vector<pax::MicroPartitionMetadata> &&micro_partitions);

  bool HasNext() const override;

  void Rewind() override;

  pax::MicroPartitionMetadata Next() override;

  ~Iterator() override;

 private:
  explicit Iterator(std::vector<pax::MicroPartitionMetadata>
                        &micro_partitions);  // NOLINT (runtime/references)

  size_t current_index_ = 0;
  std::vector<pax::MicroPartitionMetadata> micro_partitions_;
};

}  // namespace pax
