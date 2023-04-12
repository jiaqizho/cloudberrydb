#include "pax_access.h"

#include "pax_dml_state.h"
#include "comm/cbdb_wrappers.h"
#include "catalog/pax_aux_table.h"

namespace pax {
void CPaxAccess::PaxCreateAuxBlocks(const Relation relation,  const Oid relfilenode){
  cbdb::PaxCreateMicroPartitionTable(relation,relfilenode);
}
void CPaxAccess::PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                                const CommandId cid, const int options,
                                const BulkInsertState bistate) {
  CPaxInserter *inserter = CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

TableScanDesc CPaxAccess::PaxBeginScan(const Relation relation, const Snapshot snapshot,
                                       const int nkeys, const struct ScanKeyData *key,
                                       const ParallelTableScanDesc pscan,
                                       const uint32 flags) {  
  // TODO: Initial for table scan and return TableScanDesc later
  return nullptr;
}

void CPaxAccess::PaxEndScan(TableScanDesc scan) {}

bool CPaxAccess::PaxGetNextSlot(TableScanDesc scan, const ScanDirection direction,
                                TupleTableSlot *slot) {
  // TODO: implement get next tuple later
  return false;
}
}  // namespace pax
