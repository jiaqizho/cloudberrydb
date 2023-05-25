#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax.h"

namespace pax {

class PaxScanDesc {
 public:
  static TableScanDesc BeginScan(const Relation relation,
                                 const Snapshot snapshot, const int nkeys,
                                 const struct ScanKeyData *key,
                                 const ParallelTableScanDesc pscan,
                                 const uint32 flags);

  static void ReScan(TableScanDesc scan, ScanKey key, bool set_params,
                     bool allow_strat, bool allow_sync, bool allow_pagemode);
  static void EndScan(TableScanDesc scan);

  static bool ScanGetNextSlot(TableScanDesc scan, const ScanDirection direction,
                              TupleTableSlot *slot);

  static bool ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno,
                                   BufferAccessStrategy bstrategy);
  static bool ScanAnalyzeNextTuple(TableScanDesc scan, TransactionId OldestXmin,
                                   double *liverows, double *deadrows,
                                   TupleTableSlot *slot);

  static bool ScanSampleNextBlock(TableScanDesc scan,
                                  SampleScanState *scanstate);

  static bool ScanSampleNextTuple(TableScanDesc scan,
                                  SampleScanState *scanstate,
                                  TupleTableSlot *slot);

  uint32_t GetMicroPartitionNumber() const {
    return reader_->GetMicroPartitionNumber();
  }

  uint32_t GetCurrentMicroPartitionTupleNumber() const {
    return reader_->GetCurrentMicroPartitionTupleNumber();
  }

  bool SeekTuple(const uint64_t targettupleid, uint64_t *nexttupleid) {
    return reader_->SeekTuple(targettupleid, nexttupleid);
  }

  ~PaxScanDesc();

 private:
  PaxScanDesc() = default;
  static inline PaxScanDesc *to_desc(TableScanDesc scan) {
    PaxScanDesc *desc = reinterpret_cast<PaxScanDesc *>(scan);
    Assert(&desc->rs_base == scan);
    return desc;
  }

  TableScanDescData rs_base;
  const ScanKeyData *key_;
  TableReader *reader_;

  // TODO(chenhongjie): Only used by `scan analyze` and `scan sample`
  uint64_t nextTupleId = 0;
  // TODO(chenhongjie): Only used by `scan analyze`
  uint64_t targetTupleId = 0;
  // TODO(chenhongjie): Only used by `scan sample`
  uint64_t fetchTupleId = 0;
  uint64_t totalTuples = 0;
};  // class PaxScanDesc

}  // namespace pax
