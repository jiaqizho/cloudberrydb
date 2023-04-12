#include "pax_inserter.h"
#include "catalog/pax_aux_table.h"

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/local_file_system.h"
#include "storage/orc_native_micro_partition.h"

namespace pax {
CPaxInserter::CPaxInserter(Relation rel) : rel_(rel), insert_count_(0) {
  MicroPartitionWriter::WriterOptions options;
  options.relation = rel;
  options.desc = rel->rd_att;

  FileSystem *fs = Singleton<LocalFileSystem>::GetInstance();
  MicroPartitionWriter *micro_partition_writer =
      new OrcNativeMicroPartitionWriter(options, fs);
    
  micro_partition_writer->SetWriteSummaryCallback(std::bind(&CPaxInserter::AddMicroPartitionEntry, this, std::placeholders::_1));

  writer_ = new TableWriter(micro_partition_writer);
  writer_->Open();
}

CPaxInserter::~CPaxInserter() {
}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                                 CommandId cid, int options,
                                 BulkInsertState bistate) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);
  CTupleSlot cslot(slot);
  writer_->WriteTuple(&cslot);
}

void CPaxInserter::FinishInsert() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
}

void CPaxInserter::AddMicroPartitionEntry(const WriteSummary  &summary) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(rel_->rd_id, &pax_block_tables_rel_id, NULL, NULL);
  cbdb::InsertPaxBlockEntry(pax_block_tables_rel_id, summary.block_id.c_str(), summary.num_tuples);
}

}  // namespace pax