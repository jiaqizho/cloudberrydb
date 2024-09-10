#pragma once

#include "comm/cbdb_api.h"

#include <utility>

#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class MicroPartitionInfoIterator final
    : public IteratorBase<MicroPartitionMetadata> {
 public:
  MicroPartitionInfoIterator(Relation pax_rel, Snapshot snapshot,
                             const std::string &rel_path);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override { End(true); }
  ~MicroPartitionInfoIterator() = default;

 private:
  // paxc function
  void paxc_begin();
  void paxc_end(bool close_aux);

  // pax function, wrap paxc_xxx in c++
  void Begin();
  void End(bool close_aux);

  MicroPartitionMetadata ToValue(HeapTuple tuple);

  std::string rel_path_;
  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
};

class MicroPartitionInfoParallelIterator final
    : public IteratorBase<MicroPartitionMetadata> {
 public:
  MicroPartitionInfoParallelIterator(Relation pax_rel, Snapshot snapshot,
                                     ParallelBlockTableScanDesc pscan,
                                     std::string rel_path);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override { End(true); }
  ~MicroPartitionInfoParallelIterator() = default;

 private:
  MicroPartitionMetadata ToValue(HeapTuple tuple);

  // paxc function
  void paxc_begin();
  void paxc_end(bool close_aux);

  // pax function, wrap paxc_xxx in c++
  void Begin();
  void End(bool close_aux);

  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  ParallelBlockTableScanDesc pscan_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
  std::string rel_path_;

  Oid index_oid_ = InvalidOid;

  uint64 batch_allocated_ = 2;
  uint64 allocated_block_id_ = 0;
};

}  // namespace pax
