#pragma once

#include "comm/cbdb_api.h"

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "catalog/iterator.h"
#include "catalog/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"
#include "comm/paxc_utils.h"

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
    file_path.append(PAX_MICROPARTITION_DIR_POSTFIX);
    file_path.append("/");
    file_path.append(block_id);
    return file_path;
  }

  std::unique_ptr<Iterator> NewIterator();

 private:
  TableMetadata(const Relation parent_relation, const Snapshot snapshot)
      : parent_relation_(parent_relation), snapshot_(snapshot) {}

  void getAllMicroPartitionMetadata(
      std::vector<pax::MicroPartitionMetadata>& micro_partitions) {
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
  static std::unique_ptr<Iterator> Create(
      std::vector<pax::MicroPartitionMetadata>&& micro_partitions) {
    return std::unique_ptr<Iterator>(new Iterator(micro_partitions));
  }
  void Init() override {}

  std::size_t Index() const { return current_index_; }

  inline bool Empty() const override { return micro_partitions_.empty(); }

  inline uint32_t Size() const override { return micro_partitions_.size(); }

  bool HasNext() const override {
    return current_index_ + 1 < micro_partitions_.size();
  }

  size_t Seek(int offset, IteratorSeekPosType whence) override;

  pax::MicroPartitionMetadata Next() override {
    assert(current_index_ >= 0 &&
           current_index_ + 1 < micro_partitions_.size());
    return micro_partitions_[++current_index_];
  }

  pax::MicroPartitionMetadata Current() const override {
    return micro_partitions_[current_index_];
  }

  ~Iterator() { micro_partitions_.clear(); }

 private:
  explicit Iterator(std::vector<pax::MicroPartitionMetadata>&
                        micro_partitions)  // NOLINT(runtime/references)
      : current_index_(0), micro_partitions_(micro_partitions) {}

  size_t current_index_;
  std::vector<pax::MicroPartitionMetadata> micro_partitions_;
};

}  // namespace pax
