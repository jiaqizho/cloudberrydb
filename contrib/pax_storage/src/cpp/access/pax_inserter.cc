#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "catalog/pax_aux_table.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/local_file_system.h"
#include "storage/orc_native_micro_partition.h"

namespace pax {
CPaxInserter::CPaxInserter(Relation rel) : rel_(rel), insert_count_(0) {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string block_id;
  block_id = cbdb::GenRandomBlockId();
  file_path = TableMetadata::BuildPaxFilePath(rel, block_id);

  options.desc = rel->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);
  options.buffer_size = 0;

  FileSystem *fs = Singleton<LocalFileSystem>::GetInstance();
  MicroPartitionWriter *micro_partition_writer =
      new OrcNativeMicroPartitionWriter(options, fs);

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

void CPaxInserter::FinishInsert() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
}

void CPaxInserter::AddMicroPartitionEntry(const WriteSummary &summary) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(rel_->rd_id, &pax_block_tables_rel_id,
                                         NULL, NULL);
  cbdb::InsertPaxBlockEntry(pax_block_tables_rel_id, summary.block_id.c_str(),
                            summary.num_tuples, summary.file_size);
}

}  // namespace pax
