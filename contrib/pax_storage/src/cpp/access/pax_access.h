#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxAccess {
 public:
  static void PaxCreateAuxBlocks(const Relation relation);

  static void PaxTupleInsert(const Relation relation, TupleTableSlot *slot,
                             const CommandId cid, const int options,
                             const BulkInsertState bistate);
};  // class CPaxAccess

}  // namespace pax
