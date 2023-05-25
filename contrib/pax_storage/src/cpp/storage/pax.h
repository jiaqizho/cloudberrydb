#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <string>

#include "catalog/iterator.h"
#include "catalog/micro_partition_metadata.h"
#include "storage/micro_partition.h"
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
  explicit TableReader(
      const MicroPartitionReaderPtr reader,
      std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iterator)
      : iterator_(iterator), reader_(reader), is_empty_(true) {}

  virtual void Open() {
    if (!iterator_->Empty()) {
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
    slot->StoreVirtualTuple();
    return true;
  }

  std::string GetCurrentMicroPartitionId() const {
    return iterator_->Current()->getMicroPartitionId();
  }

  uint32_t GetMicroPartitionNumber() const { return iterator_->Size(); }

  uint32_t GetCurrentMicroPartitionTupleNumber() {
    return iterator_->Current()->getTupleCount();
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
    options.block_id = it->getMicroPartitionId();
    options.file_name = it->getFileName();
    reader_->Open(options);
  }

  void OpenNextFile() {
    iterator_->Next();
    OpenFile();
  }
  std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  size_t num_tuples_ = 0;
  bool is_empty_ = false;
};

}  // namespace pax
