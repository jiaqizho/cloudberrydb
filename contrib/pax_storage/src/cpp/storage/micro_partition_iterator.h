#pragma once

#include "comm/cbdb_api.h"

#include <utility>

#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class MicroPartitionIterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  virtual ~MicroPartitionIterator() = default;
  // FIXME: add flags to control which part of metadata to fetch.
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> NewParallelIterator(
      Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan);
};

}  // namespace pax
