#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "access/pax_dml_state.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_stats.h"
#include "storage/strategy.h"

namespace pax {

CPaxInserter::CPaxInserter(Relation rel)
    : rel_(rel), insert_count_(0), writer_(nullptr) {
  writer_ = std::make_unique<TableWriter>(rel);

  writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(std::make_unique<PaxDefaultSplitStrategy>())
      ->Open();
}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                               CommandId /*cid*/, int /*options*/,
                               BulkInsertState /*bistate*/) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);

  if (!TTS_IS_VIRTUAL(slot)) {
    cbdb::SlotGetAllAttrs(slot);
  }

  writer_->WriteTuple(slot);
}

void CPaxInserter::MultiInsert(Relation relation, TupleTableSlot **slots,
                               int ntuples, CommandId cid, int options,
                               BulkInsertState bistate) {
  auto inserter =
      pax::CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  for (int i = 0; i < ntuples; i++) {
    inserter->InsertTuple(relation, slots[i], cid, options, bistate);
  }
}

void CPaxInserter::FinishBulkInsert(Relation relation, int /*options*/) {
  pax::CPaxDmlStateLocal::Instance()->FinishDmlState(relation, CMD_INSERT);
}

void CPaxInserter::FinishInsert() {
  writer_->Close();
  writer_ = nullptr;
}

void CPaxInserter::TupleInsert(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  auto inserter = CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

}  // namespace pax
