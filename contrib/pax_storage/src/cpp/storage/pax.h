#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "storage/micro_partition_metadata.h"
#include "catalog/table_metadata.h"
#include "comm/bitmap.h"
#include "comm/iterator.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"
#include "storage/pax_block_id.h"
#include "storage/pax_filter.h"
#include "storage/paxc_block_map_manager.h"
#include "storage/strategy.h"

namespace pax {

class TableWriter {
 public:
  using WriteSummaryCallback = MicroPartitionWriter::WriteSummaryCallback;

  explicit TableWriter(Relation relation);

  virtual ~TableWriter();

  virtual const FileSplitStrategy *GetFileSplitStrategy() const;

  virtual void WriteTuple(CTupleSlot *slot);

  virtual void Open();

  virtual void Close();

  virtual size_t GetTotalTupleNumbers() const;

  TableWriter *SetWriteSummaryCallback(WriteSummaryCallback callback);

  TableWriter *SetFileSplitStrategy(const FileSplitStrategy *strategy);

 protected:
  virtual std::string GenFilePath(const std::string &block_id);

 protected:
  const Relation relation_;
  MicroPartitionWriter *writer_;
  const FileSplitStrategy *strategy_;
  WriteSummaryCallback summary_callback_;
  const FileSystem *file_system_ = Singleton<LocalFileSystem>::GetInstance();

  size_t num_tuples_ = 0;
  size_t total_tuples_ = 0;
};

class TableReader final {
 public:
  struct ReaderOptions {
    bool build_bitmap = false;
    Oid rel_oid = 0;

    // Will not used in TableReader
    // But pass into micro partition reader
    PaxFilter *filter = nullptr;
  };

  TableReader(MicroPartitionReader *reader,
              std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator,
              ReaderOptions options);
  virtual ~TableReader();

  void Open();

  void ReOpen();

  void Close();

  size_t GetTotalTupleNumbers() const;

  bool ReadTuple(CTupleSlot *slot);

  std::string GetCurrentMicroPartitionId() const;

  uint32 GetMicroPartitionNumber() const;

  uint32 GetCurrentMicroPartitionTupleNumber();

  uint32 GetCurrentMicroPartitionTupleOffset();

  bool SeekTuple(uint64 targettupleid, uint64 *nexttupleid);

 private:
  void SeekCurrentMicroPartitionTupleOffset(int tuple_offset);

  bool HasNextFile() const;

  void NextFile();

  void OpenFile();

  void OpenNextFile();

 private:
  const std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  size_t num_tuples_ = 0;
  bool is_empty_ = false;
  const ReaderOptions reader_options_;
  int current_block_number_ = 0;

  // only for ctid bitmap
  uint8 table_no_;
  uint32 table_index_;
};

class TableDeleter final {
 public:
  TableDeleter(
      Relation rel,
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
      std::map<std::string, std::unique_ptr<DynamicBitmap>> &&delete_bitmap,
      Snapshot snapshot);

  ~TableDeleter();

  void Delete();

 private:
  void OpenWriter();

  void OpenReader();

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
