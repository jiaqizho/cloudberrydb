#pragma once

#include "comm/cbdb_api.h"

#include <stddef.h>

#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/file_system.h"
#include "storage/micro_partition_metadata.h"
#include "storage/pax_filter.h"
#include "storage/statistics.h"

namespace pax {
class CTupleSlot {
 public:
  explicit CTupleSlot(TupleTableSlot *tuple_slot);

  inline void ClearTuple() { slot_->tts_ops->clear(slot_); }

  inline uint32 GetOffset() const { return offset_; }

  inline uint8 GetTableNo() const { return table_no_; }

  inline void SetOffset(uint64 offset) { offset_ = offset; }

  inline void SetBlockNumber(const int &block_number) {
    block_number_ = block_number;
  }

  inline void SetTableNo(uint8 table_no) { table_no_ = table_no; }

  void StoreVirtualTuple();

  TupleDesc GetTupleDesc() const;

  TupleTableSlot *GetTupleTableSlot() const;

  int GetTupleTableColumnsNum();

 private:
  TupleTableSlot *slot_;
  uint8 table_no_;
  int block_number_;
  uint32 offset_;
};

struct WriteSummary;
class MicroPartitionStats;

class MicroPartitionWriter {
 public:
  struct WriterOptions {
    std::string file_name;
    std::string block_id;
    TupleDesc desc;
    Oid rel_oid;
  };

  explicit MicroPartitionWriter(const WriterOptions &writer_options);

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

  using WriteSummaryCallback = std::function<void(const WriteSummary &summary)>;

  // summary callback is invoked after the file is closed.
  // returns MicroPartitionWriter to enable chain call.
  virtual MicroPartitionWriter *SetWriteSummaryCallback(
      WriteSummaryCallback callback);

  virtual MicroPartitionWriter *SetStatsCollector(MicroPartitionStats *mpstats);

  const WriterOptions &Options() const;

  // return the file name of the current micro partition, excluding its
  // directory path
  const std::string &FileName() const;

 protected:
  WriteSummaryCallback summary_callback_;
  const WriterOptions &writer_options_;
  FileSystem *file_system_ = nullptr;
  // only reference the mpstats, not the owner
  MicroPartitionStats *mpstats_ = nullptr;
};

template <typename T>
class DataBuffer;
class MicroPartitionReader {
 public:
  struct ReaderOptions {
    // file name(excluding directory path) for read
    std::string file_name;
    // additioinal info to initialize a reader.
    std::string block_id;

    PaxFilter *filter = nullptr;
  };

  explicit MicroPartitionReader(FileSystem *fs);

  MicroPartitionReader();

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

  virtual uint64 Offset() const = 0;

  virtual size_t Length() const = 0;

  virtual void SetReadBuffer(DataBuffer<char> *data_buffer) = 0;

 protected:
  FileSystem *file_system_ = nullptr;
};

}  // namespace pax
