#pragma once

#include <memory>
#include <optional>

#include "catalog/iterator.h"
#include "catalog/micro_partition_metadata.h"
#include "storage/micro_partition.h"

namespace pax {

class TableWriter {
 public:
  class FileSplitStrategy {
   public:
    virtual ~FileSplitStrategy() {}

    virtual bool ShouldSplit(MicroPartitionWriter *writer,
                             size_t num_tuples) = 0;
  };
  using FileSplitStrategyPtr = FileSplitStrategy *;

 public:
  TableWriter(const MicroPartitionWriterPtr writer,
              const FileSplitStrategyPtr strategy)
      : writer_(writer), strategy_(strategy) {}

  explicit TableWriter(const MicroPartitionWriterPtr writer)
      : TableWriter(writer, nullptr) {}

  virtual ~TableWriter() {
    if (writer_) {
      delete writer_;
      writer_ = nullptr;
    }

    if (strategy_) {
      delete strategy_;
      strategy_ = nullptr;
    }
  }

  virtual void WriteTuple(CTupleSlot *slot);

  virtual void Open();

  virtual void Close();

  size_t total_tuples() const;

 protected:
  MicroPartitionWriterPtr writer_;
  FileSplitStrategy *strategy_;
  size_t num_tuples_ = 0;
  size_t total_tuples_ = 0;
};

class TableReader {
 public:
  explicit TableReader(
      const MicroPartitionReaderPtr reader,
      std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iterator)
      : iterator_(iterator), reader_(reader), is_empty_(true) {}

  virtual void Open() {
    if (HasNextFile()) {
      OpenNextFile();
      is_empty_ = false;
    }
  }

  virtual void ReOpen() {
    Close();
    iterator_->Seek(0, pax::ITER_SEEK_POS_BEGIN);
    Open();
  }

  virtual void Close() {
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

 private:
  void OpenNextFile() {
    auto it = iterator_->Next();

    MicroPartitionReader::ReaderOptions options;
    options.block_id = it->getMicroPartitionId();
    options.file_name = it->getFileName();
    reader_->Open(options);
  }
  std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  size_t num_tuples_ = 0;
  bool is_empty_ = false;
};

}  // namespace pax
