#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxAccess {
 public:
  static void PaxCreateAuxBlocks(const Relation relation);
  static TableScanDesc PaxBeginScan(const Relation relation,
                                    const Snapshot snapshot, const int nkeys,
                                    const struct ScanKeyData *key,
                                    const ParallelTableScanDesc pscan,
                                    const uint32 flags);

  static void PaxEndScan(TableScanDesc scan);

  static void PaxRescan(TableScanDesc scan);

  static bool PaxGetNextSlot(TableScanDesc scan, const ScanDirection direction,
                             TupleTableSlot *slot);

  static void PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                             const CommandId cid, const int options,
                             const BulkInsertState bistate);
};  // class CPaxAccess

}  // namespace pax
