#pragma once

extern "C" {
#include "postgres.h" //NOLINT
#include "access/heapam.h"
#include "utils/relcache.h"
}

#include "storage/pax.h"

namespace pax {

class CPaxInserter {
 public:
  explicit CPaxInserter(Relation rel);
  virtual ~CPaxInserter();

  void InsertTuple(Relation relation, TupleTableSlot *slot,
                             CommandId cid, int options,
                             BulkInsertState bistate);
  void FinishInsert();
 private:
  void AddMicroPartitionEntry(const WriteSummary &summary);

  Relation rel_;
  uint32_t insert_count_;

  TableWriter* writer_;
};  // class CPaxInserter

}  // namespace pax
