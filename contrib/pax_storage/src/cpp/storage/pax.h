#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "catalog/iterator.h"
#include "catalog/micro_partition_metadata.h"
#include "catalog/table_metadata.h"
#include "comm/bitmap.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"
#include "storage/pax_block_id.h"
#include "storage/paxc_block_map_manager.h"
#include "storage/strategy.h"

namespace pax {

class TableWriter {
 public:
  using WriteSummaryCallback = MicroPartitionWriter::WriteSummaryCallback;

  explicit TableWriter(const Relation relation);

  virtual ~TableWriter();

  virtual const FileSplitStrategy *GetFileSplitStrategy() const;

  virtual void WriteTuple(CTupleSlot *slot);

  virtual void Open();

  virtual void Close();

  size_t GetTotalTupleNumbers() const;

  TableWriter *SetWriteSummaryCallback(WriteSummaryCallback callback);

  TableWriter *SetFileSplitStrategy(const FileSplitStrategy *strategy);

 protected:
  const std::string GenRandomBlockId();

  virtual std::string GenFilePath(const std::string &block_id);

 protected:
  const Relation relation_;
  MicroPartitionWriterPtr writer_;
  const FileSplitStrategy *strategy_;
  WriteSummaryCallback summary_callback_;

  size_t num_tuples_ = 0;
  size_t total_tuples_ = 0;
};

class TableReader final {
 public:
  struct ReaderOptions {
    bool build_bitmap_;
    Oid rel_oid_;
  };
  explicit TableReader(
      const MicroPartitionReaderPtr reader,
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator,
      ReaderOptions options)
      : iterator_(std::move(iterator)),
        reader_(reader),
        is_empty_(true),
        reader_options_(options) {}

  virtual ~TableReader() {
    if (reader_) {
      reader_->Close();
      delete reader_;
      reader_ = nullptr;
    }
  }

  virtual void Open() {
    if (!iterator_->Empty()) {
      if (reader_options_.build_bitmap_) {
        // first open, now alloc a table no in pax shmem for scan
        cbdb::GetTableIndexAndTableNumber(reader_options_.rel_oid_, &table_no_,
                                          &table_index_);
      }
      OpenFile();
      is_empty_ = false;
    }
  }

  virtual void ReOpen() {
    Close();
    iterator_->Seek(0, pax::BEGIN);
    Open();
  }

  virtual void Close() {
    if (is_empty_) {
      return;
    }
    Assert(reader_);
    reader_->Close();
  }

  size_t num_tuples() const { return num_tuples_; }

  virtual bool ReadTuple(CTupleSlot *slot) {
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

  std::string GetCurrentMicroPartitionId() const {
    return iterator_->Current().getMicroPartitionId();
  }

  uint32_t GetMicroPartitionNumber() const { return iterator_->Size(); }

  uint32_t GetCurrentMicroPartitionTupleNumber() {
    return iterator_->Current().getTupleCount();
  }

  uint32_t GetCurrentMicroPartitionTupleOffset() { return reader_->Offset(); }

  bool SeekTuple(const uint64_t targettupleid, uint64_t *nexttupleid) {
    uint64_t remain_num = 0;
    if (targettupleid < *nexttupleid) {
      return false;
    }

    remain_num = targettupleid - *nexttupleid + 1;
    if (remain_num <= (GetCurrentMicroPartitionTupleNumber() -
                       GetCurrentMicroPartitionTupleOffset())) {
      *nexttupleid = targettupleid;
      SeekCurrentMicroPartitionTupleOffset(
          GetCurrentMicroPartitionTupleOffset() + remain_num);
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

 private:
  void SeekCurrentMicroPartitionTupleOffset(int tuple_offset) {
    reader_->Seek(tuple_offset);
  }

  virtual bool HasNextFile() const { return iterator_->HasNext(); }

  virtual void NextFile() {
    if (reader_) {
      reader_->Close();
    }

    if (!HasNextFile()) {
      return;
    }
    OpenNextFile();
  }

  void OpenFile() {
    auto it = iterator_->Current();
    MicroPartitionReader::ReaderOptions options;
    options.block_id = it.getMicroPartitionId();
    if (reader_options_.build_bitmap_) {
      int block_number = 0;
      block_number =
          cbdb::GetBlockNumber(reader_options_.rel_oid_, table_index_,
                               paxc::PaxBlockId(options.block_id.c_str()));
      elog(DEBUG1,
           "TableReader::OpenFile ctid make mapping block_number=%d, "
           "block_id=%s",
           block_number, options.block_id.c_str());

      Assert(block_number >= 0);
      current_block_number_ = block_number;
    }
    options.file_name = it.getFileName();
    reader_->Open(options);
  }

  void OpenNextFile() {
    iterator_->Next();
    OpenFile();
  }
  const std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  size_t num_tuples_ = 0;
  bool is_empty_ = false;
  const ReaderOptions reader_options_;
  int current_block_number_ = 0;

  // only for ctid bitmap
  uint8_t table_no_;
  uint32_t table_index_;
};

class TableDeleter final {
 public:
  explicit TableDeleter(
      const Relation rel,
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
      std::map<std::string, std::unique_ptr<DynamicBitmap>> &&delete_bitmap,
      const Snapshot snapshot)
      : rel_(rel),
        iterator_(std::move(iterator)),
        delete_bitmap_(std::move(delete_bitmap)),
        snapshot_(snapshot),
        reader_(nullptr),
        writer_(nullptr),
        slot_(nullptr) {}

  ~TableDeleter() {
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

  void Delete() {
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
    for (auto it = delete_bitmap_.begin(); it != delete_bitmap_.end(); ++it) {
      auto block_id = it->first;
      cbdb::DeleteMicroPartitionEntry(rel_->rd_id, snapshot_, block_id.c_str());

      // TODO(gongxun): delete the block file
    }
  }

 private:
  void OpenWriter() {
    writer_ = new TableWriter(rel_);
    writer_->SetWriteSummaryCallback(&cbdb::AddMicroPartitionEntry)
        ->SetFileSplitStrategy(new PaxDefaultSplitStrategy())
        ->Open();
    writer_->Open();
  }

  void OpenReader() {
    FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

    // TODO(gongxun) we should put the micro partition reader inside the table
    // reader, and use a factory to create the micro partition reader.
    // MicroPartitionReaderPtr micro_partition_reader =
    //     OrcReader::CreateReader(file_system);

    MicroPartitionReader *micro_partition_reader =
        new OrcIteratorReader(file_system);

    TableReader::ReaderOptions reader_options;
    reader_options.build_bitmap_ = false;
    reader_options.rel_oid_ = rel_->rd_id;
    reader_ = new TableReader(micro_partition_reader, std::move(iterator_),
                              reader_options);
    reader_->Open();
  }

 private:
  Relation rel_;
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  std::map<std::string, std::unique_ptr<DynamicBitmap>> delete_bitmap_;
  Snapshot snapshot_;
  TableReader *reader_;
  TableWriter *writer_;
  TupleTableSlot *slot_;
};

}  // namespace pax
