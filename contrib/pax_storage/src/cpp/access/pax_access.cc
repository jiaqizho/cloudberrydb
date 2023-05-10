#include "access/pax_access.h"

#include "access/pax_deleter.h"
#include "access/pax_dml_state.h"
#include "access/pax_inserter.h"
#include "access/pax_scanner.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
void CPaxAccess::PaxCreateAuxBlocks(const Relation relation) {
  cbdb::PaxCreateMicroPartitionTable(relation);
}
void CPaxAccess::PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                                const CommandId cid, const int options,
                                const BulkInsertState bistate) {
  CPaxInserter *inserter = CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

}  // namespace pax
