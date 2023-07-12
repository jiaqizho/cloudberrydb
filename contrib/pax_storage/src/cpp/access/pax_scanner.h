#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax.h"

namespace pax {

class PaxScanDesc {
 public:
  static TableScanDesc BeginScan(Relation relation, Snapshot snapshot,
                                 int nkeys, const struct ScanKeyData *key,
                                 ParallelTableScanDesc pscan, uint32 flags);

  static void ReScan(TableScanDesc scan);
  static void EndScan(TableScanDesc scan);

  static bool ScanGetNextSlot(TableScanDesc scan, TupleTableSlot *slot);

  static bool ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno);
  static bool ScanAnalyzeNextTuple(TableScanDesc scan, double *liverows,
                                   const double *deadrows,
                                   TupleTableSlot *slot);

  static bool ScanSampleNextBlock(TableScanDesc scan,
                                  SampleScanState *scanstate);

  static bool ScanSampleNextTuple(TableScanDesc scan, TupleTableSlot *slot);

  uint32 GetMicroPartitionNumber() const;

  uint32 GetCurrentMicroPartitionTupleNumber() const;

  bool SeekTuple(uint64 target_tuple_id, uint64 *next_tuple_id);

  ~PaxScanDesc() = default;

 private:
  PaxScanDesc() = default;

  static inline PaxScanDesc *ToDesc(TableScanDesc scan) {
    auto desc = reinterpret_cast<PaxScanDesc *>(scan);
    return desc;
  }

 private:
  TableScanDescData rs_base_{};
  const ScanKeyData *key_ = nullptr;
  TableReader *reader_ = nullptr;

  DataBuffer<char> *reused_buffer_ = nullptr;
  MemoryContext memory_context_ = nullptr;

  // Only used by `scan analyze` and `scan sample`
  uint64 next_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 target_tuple_id_ = 0;
  // Only used by `scan sample`
  uint64 fetch_tuple_id_ = 0;
  uint64 total_tuples_ = 0;
};  // class PaxScanDesc

}  // namespace pax
