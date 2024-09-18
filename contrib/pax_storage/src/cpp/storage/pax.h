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

  TableWriter *SetFileSplitStrategy(
      std::unique_ptr<FileSplitStrategy> &&strategy);

  BlockNumber GetBlockNumber() const { return current_blockno_; }

 protected:
  virtual std::string GenFilePath(const std::string &block_id);

  virtual std::string GenToastFilePath(const std::string &file_path);

  virtual std::vector<std::tuple<ColumnEncoding_Kind, int>>
  GetRelEncodingOptions();

  std::vector<int> GetMinMaxColumnIndexes();

  std::vector<int> GetBloomFilterColumnIndexes();

  std::unique_ptr<MicroPartitionWriter> CreateMicroPartitionWriter(
      std::shared_ptr<MicroPartitionStats> mp_stats, bool write_only = true);

 protected:
  std::string rel_path_;
  const Relation relation_ = nullptr;
  std::unique_ptr<MicroPartitionWriter> writer_;
  std::unique_ptr<FileSplitStrategy> strategy_;
  std::shared_ptr<MicroPartitionStats> mp_stats_;
  WriteSummaryCallback summary_callback_;
  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_;

  size_t num_tuples_ = 0;
  BlockNumber current_blockno_ = 0;
  bool already_get_format_ = false;
  PaxStorageFormat storage_format_ = PaxStorageFormat::kTypeStoragePorcNonVec;
  bool already_get_min_max_col_idx_ = false;
  std::vector<int> min_max_col_idx_;
  bool already_get_bf_col_idx_ = false;
  std::vector<int> bf_col_idx_;
  bool is_dfs_table_space_;
};

class TableReader final {
 public:
  struct ReaderOptions {
    Oid table_space_id = 0;

    std::shared_ptr<DataBuffer<char>> reused_buffer;

    // Will not used in TableReader
    // But pass into micro partition reader
    std::shared_ptr<PaxFilter> filter;

#ifdef VEC_BUILD
    bool is_vec = false;
    TupleDesc tuple_desc = nullptr;
    bool vec_build_ctid = false;
#endif
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
  int GetCurrentMicroPartitionId() const { return micro_partition_id_; }

 private:
  void OpenFile();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  std::unique_ptr<MicroPartitionReader> reader_ = nullptr;
  bool is_empty_ = false;
  ReaderOptions reader_options_;
  uint32 current_block_number_ = 0;

  int micro_partition_id_;

  // only for analyze scan
  MicroPartitionMetadata current_block_metadata_;
  // only for analyze scan
  size_t current_block_row_index_ = 0;
  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_;
  bool is_dfs_table_space_;
};

class TableDeleter final {
 public:
  TableDeleter(Relation rel,
               std::map<int, std::shared_ptr<Bitmap8>> delete_bitmap,
               Snapshot snapshot);

  ~TableDeleter() = default;

  void Delete(std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);

  void DeleteWithVisibilityMap(
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
      TransactionId delete_xid);

 private:
  void UpdateStatsInAuxTable(TransactionId delete_xid,
                             const pax::MicroPartitionMetadata &meta,
                             std::shared_ptr<Bitmap8> visi_bitmap,
                             const std::vector<int> &min_max_col_idxs,
                             const std::vector<int> &bf_col_idxs,
                             std::shared_ptr<PaxFilter> filter);

  std::unique_ptr<TableWriter> OpenWriter();

  std::unique_ptr<TableReader> OpenReader(
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);

 private:
  Relation rel_;
  Snapshot snapshot_;

  std::map<int, std::shared_ptr<Bitmap8>> delete_bitmap_;

  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_;
};

}  // namespace pax
