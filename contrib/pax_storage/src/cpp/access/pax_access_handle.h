#pragma once

extern "C" {
#include "postgres.h"  // NOLINT
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "utils/snapshot.h"
}

extern "C" {
TableScanDesc pax_beginscan(Relation relation, Snapshot snapshot, int nkeys,
                            struct ScanKeyData *key,
                            ParallelTableScanDesc pscan, uint32 flags);
void pax_endscan(TableScanDesc scan);
bool pax_getnextslot(TableScanDesc scan, ScanDirection direction,
                     TupleTableSlot *slot);

void pax_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                      int options, BulkInsertState bistate);
}
