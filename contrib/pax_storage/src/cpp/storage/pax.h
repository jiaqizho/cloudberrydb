#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "comm/bitmap.h"
#include "comm/iterator.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"
#include "storage/orc/porc.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"
#include "storage/strategy.h"

#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace pax {

class TableWriter {
 public:
  using WriteSummaryCallback = MicroPartitionWriter::WriteSummaryCallback;

  explicit TableWriter(Relation relation);

  virtual ~TableWriter();

  PaxStorageFormat GetStorageFormat();

  virtual const FileSplitStrategy *GetFileSplitStrategy() const;

  virtual void WriteTuple(TupleTableSlot *slot);

  virtual void Open();

  virtual void Close();

  TableWriter *SetWriteSummaryCallback(WriteSummaryCallback callback);

  TableWriter *SetFileSplitStrategy(const FileSplitStrategy *strategy);

  TableWriter *SetStatsCollector(MicroPartitionStats *mp_stats);

  BlockNumber GetBlockNumber() const { return current_blockno_; }

 protected:
  virtual std::string GenFilePath(const std::string &block_id);

  virtual std::vector<std::tuple<ColumnEncoding_Kind, int>>
  GetRelEncodingOptions();

  MicroPartitionWriter *CreateMicroPartitionWriter(
      MicroPartitionStats *mp_stats);

 protected:
  std::string rel_path_;
  const Relation relation_ = nullptr;
  MicroPartitionWriter *writer_ = nullptr;
  const FileSplitStrategy *strategy_ = nullptr;
  MicroPartitionStats *mp_stats_ = nullptr;
  WriteSummaryCallback summary_callback_;
  const FileSystem *file_system_ = Singleton<LocalFileSystem>::GetInstance();

  size_t num_tuples_ = 0;
  BlockNumber current_blockno_ = 0;
  bool already_get_format_ = false;
  PaxStorageFormat storage_format_ = PaxStorageFormat::kTypeStoragePorcNonVec;
};

class TableReader final {
 public:
  struct ReaderOptions {
    DataBuffer<char> *reused_buffer = nullptr;

    // Will not used in TableReader
    // But pass into micro partition reader
    PaxFilter *filter = nullptr;

#ifdef VEC_BUILD
    bool is_vec = false;
    TupleDesc tuple_desc = nullptr;
    bool vec_build_ctid = false;
#endif

#ifdef ENABLE_PLASMA
    PaxCache *pax_cache = nullptr;
#endif  // ENABLE_PLASMA
  };

  TableReader(std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
              ReaderOptions options);
  virtual ~TableReader();

  void Open();

  void ReOpen();

  void Close();

  bool ReadTuple(TupleTableSlot *slot);

  bool GetTuple(TupleTableSlot *slot, ScanDirection direction,
                const size_t offset);

  // deprecate:
  // DON'T USE, this function will be removed
  const std::string &GetCurrentMicroPartitionId() const {
    return micro_partition_id_;
  }

 private:
  void OpenFile();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  bool is_empty_ = false;
  const ReaderOptions reader_options_;
  uint32 current_block_number_ = 0;

  std::string micro_partition_id_;

  // only for analyze scan
  MicroPartitionMetadata current_block_metadata_;
  // only for analyze scan
  size_t current_block_row_index_ = 0;
};

class TableDeleter final {
 public:
  TableDeleter(Relation rel,
               std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
               std::map<std::string, std::shared_ptr<Bitmap8>> delete_bitmap,
               Snapshot snapshot);

  ~TableDeleter();

  void Delete();

  // delete and mark in visibility map
  void DeleteWithVisibilityMap(TransactionId delete_xid);

 private:
  void OpenWriter();
  void OpenReader();

 private:
  Relation rel_;
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  std::map<std::string, std::shared_ptr<Bitmap8>> delete_bitmap_;
  Snapshot snapshot_;
  TableReader *reader_;
  TableWriter *writer_;
};

}  // namespace pax
