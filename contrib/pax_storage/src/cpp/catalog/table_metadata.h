#pragma once

#include "comm/cbdb_api.h"

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "catalog/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"
#include "comm/iterator.h"
#include "comm/paxc_utils.h"

namespace pax {
class TableMetadata {
 public:
  class Iterator;

  static TableMetadata *Create(const Relation parent_relation,
                               const Snapshot snapshot);

  static std::string BuildPaxFilePath(const Relation relation,
                                      const std::string &block_id);

  std::unique_ptr<Iterator> NewIterator();

 private:
  TableMetadata(const Relation parent_relation, const Snapshot snapshot);

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

  void Init() override;

  std::size_t Index() const;

  inline bool Empty() const { return micro_partitions_.empty(); }

  inline uint32 Size() const { return micro_partitions_.size(); }

  bool HasNext() const override;

  size_t Seek(int offset, IteratorSeekPosType whence) override;

  pax::MicroPartitionMetadata Next() override;

  pax::MicroPartitionMetadata Current() const override;

  ~Iterator();

 private:
  explicit Iterator(std::vector<pax::MicroPartitionMetadata>
                        &micro_partitions);  // NOLINT (runtime/references)

  size_t current_index_;
  std::vector<pax::MicroPartitionMetadata> micro_partitions_;
};

}  // namespace pax
