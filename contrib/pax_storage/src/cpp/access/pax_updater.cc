#include "access/pax_updater.h"

#include "access/pax_deleter.h"
#include "access/pax_dml_state.h"
#include "access/pax_inserter.h"

namespace pax {
TM_Result CPaxUpdater::UpdateTuple(const Relation relation,
                                   const ItemPointer otid, TupleTableSlot *slot,
                                   const CommandId cid, const Snapshot snapshot,
                                   const Snapshot crosscheck, const bool wait,
                                   TM_FailureData *tmfd,
                                   LockTupleMode *lockmode,
                                   bool *update_indexes) {
  TM_Result result;
  CPaxDeleter *deleter =
      CPaxDmlStateLocal::instance()->GetDeleter(relation, snapshot);
  Assert(deleter != nullptr);
  CPaxInserter *inserter = CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  result = deleter->MarkDelete(otid);
  // FIXME(gongxun): check result and return TM_SelfModified if needed

  inserter->InsertTuple(relation, slot, cid, 0, nullptr);
  // TODO(gongxun): update pgstat info
  return result;
}
}  // namespace pax
