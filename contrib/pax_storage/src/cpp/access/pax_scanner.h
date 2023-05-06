#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax.h"

namespace pax {
struct PaxScanDesc;

class CPaxScannner {
 public:
  static PaxScanDesc* CreateTableScanDesc(const Relation relation,
                                          const Snapshot snapshot,
                                          const int nkeys,
                                          const struct ScanKeyData* key,
                                          const ParallelTableScanDesc pscan,
                                          const uint32 flags);

  ~CPaxScannner() {}

  void ScanTableReScan(PaxScanDesc* desc);

  bool GetNextSlot(const ScanDirection direction, TupleTableSlot* slot);

  //
  bool SeekMicroPartitionOffset(int offset, IteratorSeekPosType whence) {
    return reader_->SeekMicroPartitionOffset(offset, whence);
  }

  bool SeekCurrentMicroPartitionTupleOffset(int tuple_offset) {
    return reader_->SeekCurrentMicroPartitionTupleOffset(tuple_offset);
  }

  uint32_t GetMicroPartitionNumber() const {
    return reader_->GetMicroPartitionNumber();
  }

  uint32_t GetCurrentMicroPartitionTupleNumber() const {
    return reader_->GetCurrentMicroPartitionTupleNumber();
  }

  bool SeekTuple(const uint64_t targettupleid, uint64_t *nexttupleid) {
    return reader_->SeekTuple(targettupleid, nexttupleid);
  }

 private:
  CPaxScannner(const TableScanDesc scan_desc, const ScanKeyData* key);
  CPaxScannner() = delete;

  TableReader* reader_;
  TableScanDesc scan_desc_;
  const ScanKeyData* key_;
};  // class CPaxScanner

struct PaxScanDesc {
  TableScanDescData rs_base;
  CPaxScannner* scanner;

  // TODO(chenhongjie): Only used by `scan analyze` and `scan sample`
  uint64_t nextTupleId;
  // TODO(chenhongjie): Only used by `scan analyze`
  uint64_t targetTupleId;
  // TODO(chenhongjie): Only used by `scan sample`
  uint64_t fetchTupleId;
  uint64_t totalTuples;
};

}  // namespace pax
