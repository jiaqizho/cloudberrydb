#pragma once

#include "comm/cbdb_api.h"

#include <stddef.h>

#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "comm/pax_def.h"
#include "storage/file_system.h"
#include "storage/statistics.h"

namespace pax {

class CTupleSlot {
 public:
  explicit CTupleSlot(TupleTableSlot *tuple_slot) : slot_(tuple_slot) {}
  TupleTableSlot *GetTupleTableSlot() { return slot_; }

  void ClearTuple() { slot_->tts_ops->clear(slot_); }

  void StoreVirtualTuple() {
    slot_->tts_flags &= ~TTS_FLAG_EMPTY;
    slot_->tts_nvalid = slot_->tts_tupleDescriptor->natts;
  }

  TupleDesc GetTupleDesc() const { return slot_->tts_tupleDescriptor; }

  TupleTableSlot *GetTupleTableSlot() const { return slot_; }

 private:
  TupleTableSlot *slot_;
};

// WriteSummary is generated after the current micro partition is flushed and
// closed.
struct WriteSummary {
  std::string file_name;
  std::string block_id;
  size_t file_size;
  size_t num_tuples;
  WriteSummary() : file_size(0), num_tuples(0) {}
  WriteSummary(const WriteSummary &summary)
      : file_name(summary.file_name),
        block_id(summary.block_id),
        file_size(summary.file_size),
        num_tuples(summary.num_tuples) {}
};

class MicroPartitionWriter;
using MicroPartitionWriterPtr = pax::MicroPartitionWriter *;

class MicroPartitionWriter {
 public:
  struct WriterOptions {
    std::string file_name;
    std::string block_id;
    TupleDesc desc;
  };

  explicit MicroPartitionWriter(const WriterOptions &writer_options)
      : writer_options_(writer_options) {}

  virtual ~MicroPartitionWriter() = default;

  // close the current write file. Create may be called after Close
  // to write a new micro partition.
  virtual void Close() = 0;

  // immediately, flush memory data into file system
  virtual void Flush() = 0;

  // estimated size of the writing size, used to determine
  // whether to switch to another micro partition.
  virtual size_t EstimatedSize() const = 0;

  // append tuple to the current micro partition file
  // return the number of tuples the current micro partition has written
  virtual void WriteTuple(CTupleSlot *slot) = 0;
  virtual void WriteTupleN(CTupleSlot **slot, size_t n) = 0;

  // returns the full file name(including directory path) used for write.
  // normally it's <tablespace_dir>/<database_id>/<file_name> for local files,
  // depends on the write options
  virtual const std::string FullFileName() const = 0;

  using WriteSummaryCallback = std::function<void(const WriteSummary &summary)>;

  // summary callback is invoked after the file is closed.
  // returns MicroPartitionWriter to enable chain call.
  virtual MicroPartitionWriter *SetWriteSummaryCallback(
      WriteSummaryCallback callback);

  virtual MicroPartitionWriter *SetStatsCollector(StatsCollector *collector);

  const WriterOptions &Options() const;

  // return the file name of the current micro partition, excluding its
  // directory path
  const std::string &FileName() const;

 protected:
  WriteSummaryCallback summary_callback_;
  const WriterOptions &writer_options_;
  FileSystem *file_system_ = nullptr;
  StatsCollector *collector_ = nullptr;
};

class MicroPartitionReader;
using MicroPartitionReaderPtr = pax::MicroPartitionReader *;

class MicroPartitionReader {
 public:
  struct ReaderOptions {
    // file name(excluding directory path) for read
    std::string file_name;
    // additioinal info to initialize a reader.
    std::string block_id;
  };

  explicit MicroPartitionReader(const FileSystemPtr fs) : file_system_(fs) {}

  MicroPartitionReader() : file_system_(nullptr) {}

  virtual ~MicroPartitionReader() = default;

  virtual void Open(const ReaderOptions &options) = 0;

  // Close the current reader. It may be re-Open.
  virtual void Close() = 0;

  // read tuple from the micro partition with a filter.
  // the default behavior doesn't push the predicate down to
  // the low-level storage code.
  // returns the offset of the tuple in the micro partition
  // NOTE: the ctid is stored in slot, mapping from block_id to micro partition
  // is also created during this stage, no matter the map relation is needed or
  // not. We may optimize to avoid creating the map relation later.
  virtual bool ReadTuple(CTupleSlot *slot) = 0;

  virtual void Seek(size_t offset) = 0;

  virtual uint64 Offset() const = 0;

  virtual size_t Length() const = 0;

  using Filter = Vector<uint16_t>;
  virtual void SetFilter(Filter *filter) = 0;

 protected:
  const FileSystemPtr file_system_ = nullptr;
};

}  // namespace pax
