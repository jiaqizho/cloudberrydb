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

  uint32 GetMicroPartitionNumber() const;

  uint32 GetCurrentMicroPartitionTupleNumber() const;

  bool SeekTuple(const uint64 target_tuple_id, uint64 *next_tuple_id);

  ~PaxScanDesc() = default;

 private:
  PaxScanDesc() = default;

  static inline PaxScanDesc *to_desc(TableScanDesc scan) {
    PaxScanDesc *desc = reinterpret_cast<PaxScanDesc *>(scan);
    return desc;
  }

 private:
  TableScanDescData rs_base_;
  const ScanKeyData *key_;
  TableReader *reader_;

  DataBuffer<char> *reused_buffer_;

  // Only used by `scan analyze` and `scan sample`
  uint64 next_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 target_tuple_id_ = 0;
  // Only used by `scan sample`
  uint64 fetch_tuple_id_ = 0;
  uint64 total_tuples_ = 0;
};  // class PaxScanDesc

}  // namespace pax
