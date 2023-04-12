#pragma once

extern "C" {
#include "postgres.h"  // NOLINT
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "utils/snapshot.h"
}

namespace pax {
class CPaxAccess {
 public:
  static void PaxCreateAuxBlocks(const Relation relation,
                                 const Oid relfilenode);

  static TableScanDesc PaxBeginScan(const Relation relation,
                                    const Snapshot snapshot, const int nkeys,
                                    const struct ScanKeyData *key,
                                    const ParallelTableScanDesc pscan,
                                    const uint32 flags);

  static void PaxEndScan(TableScanDesc scan);

  static bool PaxGetNextSlot(TableScanDesc scan, const ScanDirection direction,
                             TupleTableSlot *slot);

  static void PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                             const CommandId cid, const int options,
                             const BulkInsertState bistate);

};  // class CPaxAccess

}  // namespace pax

extern "C" {}