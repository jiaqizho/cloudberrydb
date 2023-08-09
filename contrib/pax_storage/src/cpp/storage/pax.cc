#include "storage/pax.h"

#include <uuid/uuid.h>

#include <utility>

#include "catalog/micro_partition_metadata.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_file_factory.h"

namespace pax {

static std::string GenRandomBlockId() {
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
}

TableWriter::TableWriter(Relation relation)
    : relation_(relation),
      writer_(nullptr),
      strategy_(nullptr),
      summary_callback_(nullptr) {}

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
  return file_system_->BuildPaxFilePath(relation_, block_id);
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

  File *file =
      Singleton<LocalFileSystem>::GetInstance()->Open(options.file_name);

  writer_ = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      MICRO_PARTITION_TYPE_PAX, file, std::move(options));

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

size_t TableWriter::GetTotalTupleNumbers() const { return total_tuples_; }

void TableWriter::Close() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
  num_tuples_ = 0;
}

TableReader::TableReader(
    MicroPartitionReader *reader,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator,
    ReaderOptions options)
    : iterator_(std::move(iterator)),
      reader_(reader),
      is_empty_(true),
      reader_options_(options),
      table_no_(0),
      table_index_(0) {}

TableReader::~TableReader() {
  if (reader_) {
    reader_->Close();
    delete reader_;
    reader_ = nullptr;
  }
}

void TableReader::Open() {
  if (!iterator_->Empty()) {
    if (reader_options_.build_bitmap) {
      // first open, now alloc a table no in pax shmem for scan
      cbdb::GetTableIndexAndTableNumber(reader_options_.rel_oid, &table_no_,
                                        &table_index_);
    }
    OpenFile();
    is_empty_ = false;
  }
}

void TableReader::ReOpen() {
  Close();
  iterator_->Seek(0, pax::BEGIN);
  Open();
}

void TableReader::Close() {
  if (is_empty_) {
    return;
  }
  Assert(reader_);
  reader_->Close();
}

size_t TableReader::GetTotalTupleNumbers() const { return num_tuples_; }

bool TableReader::ReadTuple(CTupleSlot *slot) {
  if (is_empty_) {
    return false;
  }

  slot->ClearTuple();
  while (!reader_->ReadTuple(slot)) {
    if (!HasNextFile()) {
      return false;
    }
    NextFile();
  }
  num_tuples_++;
  slot->SetTableNo(table_no_);
  slot->SetBlockNumber(current_block_number_);
  slot->StoreVirtualTuple();
  return true;
}

std::string TableReader::GetCurrentMicroPartitionId() const {
  return iterator_->Current().GetMicroPartitionId();
}

uint32 TableReader::GetMicroPartitionNumber() const {
  return iterator_->Size();
}

uint32 TableReader::GetCurrentMicroPartitionTupleNumber() {
  return iterator_->Current().GetTupleCount();
}

uint32 TableReader::GetCurrentMicroPartitionTupleOffset() {
  return reader_->Offset();
}

bool TableReader::SeekTuple(const uint64 targettupleid, uint64 *nexttupleid) {
  uint64 remain_num = 0;
  if (targettupleid < *nexttupleid) {
    return false;
  }

  remain_num = targettupleid - *nexttupleid + 1;
  if (remain_num <= (GetCurrentMicroPartitionTupleNumber() -
                     GetCurrentMicroPartitionTupleOffset())) {
    *nexttupleid = targettupleid;
    SeekCurrentMicroPartitionTupleOffset(GetCurrentMicroPartitionTupleOffset() +
                                         remain_num);
    return true;
  }

  do {
    remain_num -= (GetCurrentMicroPartitionTupleNumber() -
                   GetCurrentMicroPartitionTupleOffset());
    if (HasNextFile()) {
      NextFile();
    } else {
      return false;
    }
  } while (remain_num > GetCurrentMicroPartitionTupleNumber());

  *nexttupleid = targettupleid;
  SeekCurrentMicroPartitionTupleOffset(remain_num);
  return true;
}

void TableReader::SeekCurrentMicroPartitionTupleOffset(int tuple_offset) {
  reader_->Seek(tuple_offset);
}

bool TableReader::HasNextFile() const { return iterator_->HasNext(); }

void TableReader::NextFile() {
  if (reader_) {
    reader_->Close();
  }

  if (!HasNextFile()) {
    return;
  }
  OpenNextFile();
}

void TableReader::OpenFile() {
  auto it = iterator_->Current();
  MicroPartitionReader::ReaderOptions options;
  options.block_id = it.GetMicroPartitionId();
  if (reader_options_.build_bitmap) {
    int block_number = 0;
    block_number =
        cbdb::GetBlockNumber(reader_options_.rel_oid, table_index_,
                             paxc::PaxBlockId(options.block_id.c_str()));
    elog(DEBUG1,
         "TableReader::OpenFile ctid make mapping block_number=%d, "
         "block_id=%s",
         block_number, options.block_id.c_str());

    Assert(block_number >= 0);
    current_block_number_ = block_number;
  }
  options.file_name = it.GetFileName();
  options.filter = reader_options_.filter;
  reader_->Open(options);
}

void TableReader::OpenNextFile() {
  iterator_->Next();
  OpenFile();
}

TableDeleter::TableDeleter(
    Relation rel,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    std::map<std::string, std::unique_ptr<DynamicBitmap>> &&delete_bitmap,
    Snapshot snapshot)
    : rel_(rel),
      iterator_(std::move(iterator)),
      delete_bitmap_(std::move(delete_bitmap)),
      snapshot_(snapshot),
      reader_(nullptr),
      writer_(nullptr),
      slot_(nullptr) {}

TableDeleter::~TableDeleter() {
  if (reader_) {
    reader_->Close();
    delete reader_;
    reader_ = nullptr;
  }

  if (writer_) {
    writer_->Close();
    delete writer_;
    writer_ = nullptr;
  }
  if (slot_) {
    ExecDropSingleTupleTableSlot(slot_);
  }
}

void TableDeleter::Delete() {
  if (iterator_->Empty()) {
    return;
  }
  slot_ = MakeTupleTableSlot(rel_->rd_att, &TTSOpsVirtual);
  OpenReader();
  OpenWriter();

  CTupleSlot cslot(slot_);
  // TODO(gongxun): because bulk insert as AO/HEAP does with tuples iteration
  // not implemented. we should implement bulk insert firstly. and then we can
  // use ReadTupleN and WriteTupleN to delete tuples in batch.
  while (reader_->ReadTuple(&cslot)) {
    auto block_id = reader_->GetCurrentMicroPartitionId();
    auto it = delete_bitmap_.find(block_id);
    if (it == delete_bitmap_.end()) {
      // should not be here
      Assert(!"should not be here, block_id is marked as delete but not in "
                "delete_bitmap_");
      continue;
    }

    auto bitmap = it->second.get();
    if (cslot.GetOffset() < bitmap->NumBits() &&
        bitmap->Test(cslot.GetOffset())) {
      continue;
    }
    writer_->WriteTuple(&cslot);
  }

  // loop delete_bitmap
  for (const auto &it : delete_bitmap_) {
    auto block_id = it.first;
    cbdb::DeleteMicroPartitionEntry(rel_->rd_id, snapshot_, block_id);

    // TODO(gongxun): delete the block file
  }
}

void TableDeleter::OpenWriter() {
  writer_ = new TableWriter(rel_);
  writer_->SetWriteSummaryCallback(&cbdb::AddMicroPartitionEntry)
      ->SetFileSplitStrategy(new PaxDefaultSplitStrategy())
      ->Open();
  writer_->Open();
}

void TableDeleter::OpenReader() {
  auto file_system = Singleton<LocalFileSystem>::GetInstance();

  // TODO(gongxun) we should put the micro partition reader inside the table
  // reader, and use a factory to create the micro partition reader.
  // MicroPartitionReader * micro_partition_reader =
  //     OrcReader::CreateReader(file_system);

  MicroPartitionReader *micro_partition_reader =
      new OrcIteratorReader(file_system);

  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = rel_->rd_id;
  reader_ = new TableReader(micro_partition_reader, std::move(iterator_),
                            reader_options);
  reader_->Open();
}

}  // namespace pax
