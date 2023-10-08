#pragma once

#include "comm/cbdb_api.h"

#include <stddef.h>

#include <functional>
#include <stdexcept>
#include <string>

#include "storage/columns/pax_columns.h"
#include "storage/micro_partition_metadata.h"
#include "storage/pax_defined.h"

namespace pax {
class CTupleSlot {
 public:
  explicit CTupleSlot(TupleTableSlot *tuple_slot);

  inline void ClearTuple() { slot_->tts_ops->clear(slot_); }

  inline uint32 GetOffset() const { return offset_; }

  inline uint8 GetTableNo() const { return table_no_; }

  inline int GetBlockNumber() const { return block_number_; }

  inline void SetOffset(uint64 offset) { offset_ = offset; }

  inline void SetBlockNumber(const int &block_number) {
    block_number_ = block_number;
  }

  inline void SetTableNo(uint8 table_no) { table_no_ = table_no; }

  void StoreVirtualTuple();

  TupleDesc GetTupleDesc() const;

  TupleTableSlot *GetTupleTableSlot() const;

 private:
  TupleTableSlot *slot_;
  uint8 table_no_;
  int block_number_;
  uint32 offset_;
};

struct WriteSummary;
class FileSystem;
class MicroPartitionStats;
class PaxFilter;

class MicroPartitionWriter {
 public:
  struct WriterOptions {
    std::string file_name;
    std::string block_id;
    TupleDesc desc;
    Oid rel_oid;
    std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

    size_t group_limit = VEC_BATCH_LENGTH;
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
  virtual size_t PhysicalSize() const = 0;

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

 protected:
  WriteSummaryCallback summary_callback_;
  WriterOptions writer_options_;
  FileSystem *file_system_ = nullptr;
  // only reference the mpstats, not the owner
  MicroPartitionStats *mpstats_ = nullptr;
};

#ifdef ENABLE_PLASMA
class PaxCache;
#endif

template <typename T>
class DataBuffer;

class MicroPartitionReader {
 public:
  class Group {
   public:
    virtual ~Group() = default;

    virtual size_t GetRows() const = 0;

    virtual size_t GetRowOffset() const = 0;

    // `ReadTuple` is the same interface in the `MicroPartitionReader`
    // this interface at the group level, if no rows remain in current
    // group, then the first value in return std::pair will be `false`.
    //
    // the secord value in return std::pair is the row offset of current
    // group.
    virtual std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) = 0;

    // ------------------------------------------
    // The below interfaces is used to directly access
    // the pax columns in the group.
    // Other `MicroPartitionReader` can quickly perform
    // some operations, like filter, convert format...
    // ------------------------------------------
    virtual bool GetTuple(TupleTableSlot *slot, size_t row_index) = 0;

    virtual std::pair<Datum, bool> GetColumnValue(size_t column_index,
                                                  size_t row_index) = 0;
    virtual std::pair<Datum, bool> GetColumnValue(PaxColumn *column,
                                                  size_t row_index) = 0;

    // Allow different MicroPartitionReader shared columns
    // but should not let export columns out of micro partition
    //
    // In MicroPartition writer/reader implementation, all in-memory data should
    // be accessed by pax column This is because most of the common logic of
    // column operation is done in pax column, such as type mapping, bitwise
    // fetch, compression/encoding. At the same time, pax column can also be
    // used as a general interface for internal using, because it's zero copy
    // from buffer. more details in `storage/columns`
    virtual PaxColumns *GetAllColumns() const = 0;
  };

  struct ReaderOptions {
    // file name(excluding directory path) for read
    std::string file_name;
    // additioinal info to initialize a reader.
    std::string block_id;

    // Optional, when reused buffer is not set, new memory will be generated for
    // ReadTuple
    DataBuffer<char> *reused_buffer = nullptr;

    PaxFilter *filter = nullptr;
#ifdef ENABLE_PLASMA
    PaxCache *pax_cache = nullptr;
#endif  // ENABLE_PLASMA
  };

  MicroPartitionReader() = default;

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

  // ------------------------------------------
  // below interface different with `ReadTuple`
  //
  // direct read with `Group` from current
  // `MicroPartitionReader` with group index.
  // The group index will not be changed, and
  // won't have any middle state in this process.
  // ------------------------------------------
  virtual size_t GetGroupNums() = 0;

  virtual Group *ReadGroup(size_t group_index) = 0;

#ifdef VEC_BUILD
 private:
  friend class PaxVecReader;
#endif
};

}  // namespace pax
