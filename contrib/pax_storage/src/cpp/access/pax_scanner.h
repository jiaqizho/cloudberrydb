#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax.h"
#include "storage/pax_filter.h"
#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif
namespace pax {

class PaxScanDesc {
 public:
  static TableScanDesc BeginScan(Relation relation, Snapshot snapshot,
                                 int nkeys, struct ScanKeyData *key,
                                 ParallelTableScanDesc pscan, uint32 flags,
                                 PaxFilter *filter);

  static void ReScan(TableScanDesc scan);
  static void EndScan(TableScanDesc scan);

  static TableScanDesc BeginScanExtractColumns(
      Relation rel, Snapshot snapshot, ParallelTableScanDesc parallel_scan,
      List *targetlist, List *qual, uint32 flags);

  static bool ScanGetNextSlot(TableScanDesc scan, TupleTableSlot *slot);

  static bool ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno);
  static bool ScanAnalyzeNextTuple(TableScanDesc scan, double *liverows,
                                   const double *deadrows,
                                   TupleTableSlot *slot);

  static bool ScanSampleNextBlock(TableScanDesc scan,
                                  SampleScanState *scanstate);

  static bool ScanSampleNextTuple(TableScanDesc scan, TupleTableSlot *slot);

  ~PaxScanDesc() = default;

 private:
  PaxScanDesc() = default;

  static inline PaxScanDesc *ScanToDesc(TableScanDesc scan) {
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

  // filter used to do column projection
  PaxFilter *filter_ = nullptr;
#ifdef VEC_BUILD
  VecAdapter *vec_adapter_ = nullptr;
#endif

#ifdef ENABLE_PLASMA
  const std::string plasma_socket_path_prefix_ = "/tmp/.s.plasma.";
  PaxCache *pax_cache_ = nullptr;
#endif
};  // class PaxScanDesc

}  // namespace pax
