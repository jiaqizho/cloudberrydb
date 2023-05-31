#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "access/pax_dml_state.h"
#include "catalog/pax_aux_table.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "storage/strategy.h"

namespace pax {

CPaxInserter::CPaxInserter(Relation rel) : rel_(rel), insert_count_(0) {
  writer_ = new TableWriter(rel);
  writer_->SetWriteSummaryCallback(&cbdb::AddMicroPartitionEntry)
      ->SetFileSplitStrategy(new PaxDefaultSplitStrategy())
      ->Open();
}

CPaxInserter::~CPaxInserter() {}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);

  if (!TTS_IS_VIRTUAL(slot)) {
    slot_getallattrs(slot);
  }

  CTupleSlot cslot(slot);
  writer_->WriteTuple(&cslot);
}

void CPaxInserter::MultiInsert(Relation relation, TupleTableSlot **slots,
                               int ntuples, CommandId cid, int options,
                               BulkInsertState bistate) {
  CPaxInserter *inserter =
      pax::CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);
  // TODO(Tony): implement bulk insert as AO/HEAP does with tuples iteration,
  // which needs to be futher optmized by using new feature like Parallelization
  // or Vectorization.
  for (int i = 0; i < ntuples; i++) {
    inserter->InsertTuple(relation, slots[i], cid, options, bistate);
  }
}

void CPaxInserter::FinishBulkInsert(Relation relation, int options) {
  pax::CPaxDmlStateLocal::instance()->FinishDmlState(relation, CMD_INSERT);
}

void CPaxInserter::FinishInsert() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
}

void CPaxInserter::TupleInsert(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  CPaxInserter *inserter = CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

}  // namespace pax
