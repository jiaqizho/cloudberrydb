#include "access/pax_access.h"

#include "access/pax_dml_state.h"
#include "access/pax_scanner.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
void CPaxAccess::PaxCreateAuxBlocks(const Relation relation,
                                    const Oid relfilenode) {
  cbdb::PaxCreateMicroPartitionTable(relation, relfilenode);
}
void CPaxAccess::PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                                const CommandId cid, const int options,
                                const BulkInsertState bistate) {
  CPaxInserter *inserter = CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

TableScanDesc CPaxAccess::PaxBeginScan(const Relation relation,
                                       const Snapshot snapshot, const int nkeys,
                                       const struct ScanKeyData *key,
                                       const ParallelTableScanDesc pscan,
                                       const uint32 flags) {
  // TODO(gongxun): Initial for table scan and return TableScanDesc later
  PaxScanDesc *desc;
  desc = CPaxScannner::CreateTableScanDesc(relation, snapshot, nkeys, key,
                                           pscan, flags);

  return reinterpret_cast<TableScanDesc>(desc);
}

void CPaxAccess::PaxEndScan(TableScanDesc scan) {
  PaxScanDesc *desc = reinterpret_cast<PaxScanDesc *>(scan);
  Assert(desc->scanner != nullptr);
  delete desc->scanner;
  delete desc;
}

bool CPaxAccess::PaxGetNextSlot(TableScanDesc scan,
                                const ScanDirection direction,
                                TupleTableSlot *slot) {
  PaxScanDesc *desc = reinterpret_cast<PaxScanDesc *>(scan);
  Assert(desc->scanner != nullptr);
  return desc->scanner->GetNextSlot(direction, slot);
}
}  // namespace pax
