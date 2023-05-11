#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "access/pax_dml_state.h"
#include "catalog/pax_aux_table.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/local_file_system.h"
#include "storage/orc/orc.h"

namespace pax {

CPaxInserter::CPaxInserter(Relation rel) : rel_(rel), insert_count_(0) {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string block_id;
  block_id = cbdb::GenRandomBlockId();
  file_path = TableMetadata::BuildPaxFilePath(rel, block_id);

  FileSystem *fs = Singleton<LocalFileSystem>::GetInstance();

  options.desc = rel->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);
  options.buffer_size = 0;

  MicroPartitionWriter *micro_partition_writer =
      OrcWriter::CreateWriter(fs, std::move(options));

  micro_partition_writer->SetWriteSummaryCallback(std::bind(
      &CPaxInserter::AddMicroPartitionEntry, this, std::placeholders::_1));

  writer_ = new TableWriter(micro_partition_writer);
  writer_->Open();
}

CPaxInserter::~CPaxInserter() {}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);
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

void CPaxInserter::AddMicroPartitionEntry(const WriteSummary &summary) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(rel_->rd_id, &pax_block_tables_rel_id,
                                         NULL, NULL);
  cbdb::InsertPaxBlockEntry(pax_block_tables_rel_id, summary.block_id.c_str(),
                            summary.num_tuples, summary.file_size);
}

}  // namespace pax
