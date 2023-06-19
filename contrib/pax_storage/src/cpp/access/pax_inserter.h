#pragma once

#include "comm/cbdb_api.h"

#include "catalog/micro_partition_metadata.h"
#include "storage/pax.h"

namespace pax {

class CPaxInserter {
 public:
  explicit CPaxInserter(Relation rel);
  virtual ~CPaxInserter() = default;

  static void TupleInsert(Relation relation, TupleTableSlot *slot,
                          CommandId cid, int options, BulkInsertState bistate);

  static void MultiInsert(Relation relation, TupleTableSlot **slots,
                          int ntuples, CommandId cid, int options,
                          BulkInsertState bistate);

  static void FinishBulkInsert(Relation relation, int options);

  void InsertTuple(Relation relation, TupleTableSlot *slot, CommandId cid,
                   int options, BulkInsertState bistate);
  void FinishInsert();

 private:
  Relation rel_;
  uint32 insert_count_;

  TableWriter *writer_;
};  // class CPaxInserter

}  // namespace pax
