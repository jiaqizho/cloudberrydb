#pragma once
#include <memory>
#include <string>
#include <vector>

#include "catalog/iterator.h"
#include "catalog/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"

extern "C" {
#include "postgres.h"  // NOLINT
#include "utils/relcache.h"
#include "utils/snapshot.h"
}

namespace pax {
class TableMetadata {
 public:
  class Iterator;

  static TableMetadata* Create(const Relation parent_relation,
                               const Snapshot snapshot) {
    return new TableMetadata(parent_relation, snapshot);
  }

  static std::string BuildPaxFilePath(const Relation relation,
                                      const std::string block_id) {
    std::string file_path;
    std::string base_path =
        GetDatabasePath(relation->rd_node.dbNode, relation->rd_node.spcNode);
    file_path.append(std::string(DataDir));
    file_path.append("/");
    file_path.append(std::string(base_path));
    file_path.append("/");
    file_path.append(std::to_string(relation->rd_node.relNode));
    file_path.append("_");
    file_path.append(block_id);
    return file_path;
  }

  std::shared_ptr<Iterator> NewIterator();

 private:
  TableMetadata(const Relation parent_relation, const Snapshot snapshot)
      : parent_relation_(parent_relation), snapshot_(snapshot) {}

  void getAllMicroPartitionMetadata(
      std::shared_ptr<std::vector<std::shared_ptr<MicroPartitionMetadata>>>
          micro_partitions) {
    cbdb::GetAllMicroPartitionMetadata(parent_relation_, snapshot_,
                                       micro_partitions);
  }

 private:
  const Relation parent_relation_;
  const Snapshot snapshot_;
};  // class TableMetadata

// TODO(gongxun): enhance this iterator to support lazy loading
class TableMetadata::Iterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  static std::shared_ptr<Iterator> Create(
      std::shared_ptr<std::vector<std::shared_ptr<MicroPartitionMetadata>>>
          micro_partitions) {
    return std::shared_ptr<Iterator>(new Iterator(micro_partitions));
  }
  void Init() override {}
  bool HasNext() override {
    return !micro_partitions_->empty() &&
           current_index_ < micro_partitions_->size();
  }

  std::shared_ptr<MicroPartitionMetadata>& Next() override {
    return micro_partitions_->at(current_index_++);
  }

  ~Iterator() { micro_partitions_->clear(); }

 private:
  Iterator(std::shared_ptr<std::vector<std::shared_ptr<MicroPartitionMetadata>>>
               micro_partitions)
      : current_index_(0), micro_partitions_(micro_partitions) {}

  size_t current_index_;
  std::shared_ptr<std::vector<std::shared_ptr<MicroPartitionMetadata>>>
      micro_partitions_;
};

}  // namespace pax
