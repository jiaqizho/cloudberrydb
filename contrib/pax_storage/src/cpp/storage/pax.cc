#include "storage/pax.h"

#include <uuid/uuid.h>

#include <utility>

#include "catalog/micro_partition_metadata.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "storage/local_file_system.h"
#include "storage/orc/orc.h"

namespace pax {

TableWriter::TableWriter(const Relation relation)
    : relation_(relation), strategy_(nullptr), summary_callback_(nullptr) {}

TableWriter *TableWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  Assert(!summary_callback_);
  summary_callback_ = callback;
  return this;
}

TableWriter *TableWriter::SetFileSplitStrategy(
    const FileSplitStrategy *strategy) {
  Assert(!strategy_);
  strategy_ = strategy;
  return this;
}

TableWriter::~TableWriter() {
  // must call close before delete table writer
  Assert(writer_ == nullptr);

  delete strategy_;
  strategy_ = nullptr;
}

const FileSplitStrategy *TableWriter::GetFileSplitStrategy() const {
  return strategy_;
}

std::string TableWriter::GenFilePath(const std::string &block_id) {
  return TableMetadata::BuildPaxFilePath(relation_, block_id);
}

void TableWriter::Open() {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string block_id;

  Assert(strategy_);
  Assert(summary_callback_);

  block_id = GenRandomBlockId();
  file_path = GenFilePath(block_id);

  options.rel_oid = relation_->rd_id;
  options.desc = relation_->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);

  writer_ = OrcWriter::CreateWriter(Singleton<LocalFileSystem>::GetInstance(),
                                    std::move(options));

  writer_->SetWriteSummaryCallback(summary_callback_);
}

void TableWriter::WriteTuple(CTupleSlot *slot) {
  Assert(writer_);
  Assert(strategy_);
  // should check split strategy before write tuple
  // otherwise, may got a empty file in the disk
  if (strategy_->ShouldSplit(writer_, num_tuples_)) {
    this->Close();
    this->Open();
  }

  writer_->WriteTuple(slot);
  ++num_tuples_;
  ++total_tuples_;
}

const std::string TableWriter::GenRandomBlockId() {
  CBDB_WRAP_START;
  {
    uuid_t uuid;
    char str[36] = {0};

    uuid_generate(uuid);
    uuid_unparse(uuid, str);

    std::string uuid_str = str;
    return uuid_str;
  }
  CBDB_WRAP_END;
  return nullptr;
}

size_t TableWriter::GetTotalTupleNumbers() const { return total_tuples_; }

void TableWriter::Close() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
  num_tuples_ = 0;
}

}  // namespace pax
