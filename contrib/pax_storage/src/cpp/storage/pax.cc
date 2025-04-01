#include "storage/pax.h"

#include <map>
#include <utility>

#include "access/pax_visimap.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_encoding.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/micro_partition_stats.h"
#include "storage/remote_file_system.h"

#ifdef VEC_BUILD
#include "storage/vec/pax_vec_reader.h"
#endif

namespace paxc {
class IndexUpdaterInternal {
 public:
  void Begin(Relation rel) {
    Assert(rel);

    rel_ = rel;
    slot_ = MakeTupleTableSlot(rel->rd_att, &TTSOpsVirtual);

    if (HasIndex()) {
      estate_ = CreateExecutorState();

      relinfo_ = makeNode(ResultRelInfo);
      relinfo_->ri_RelationDesc = rel;
      ExecOpenIndices(relinfo_, false);
    }
  }

  void UpdateIndex(TupleTableSlot *slot) {
    Assert(slot == slot_);
    Assert(HasIndex());
    auto recheck_index =
        ExecInsertIndexTuples(relinfo_, slot_, estate_, true, false, NULL, NIL);
    list_free(recheck_index);
  }

  void End() {
    if (HasIndex()) {
      Assert(relinfo_ && estate_);

      ExecCloseIndices(relinfo_);
      pfree(relinfo_);
      relinfo_ = nullptr;

      FreeExecutorState(estate_);
      estate_ = nullptr;
    }
    Assert(relinfo_ == nullptr && estate_ == nullptr);

    ExecDropSingleTupleTableSlot(slot_);
    slot_ = nullptr;

    rel_ = nullptr;
  }

  inline TupleTableSlot *GetSlot() { return slot_; }
  inline bool HasIndex() const { return rel_->rd_rel->relhasindex; }

 private:
  Relation rel_ = nullptr;
  TupleTableSlot *slot_ = nullptr;
  EState *estate_ = nullptr;
  ResultRelInfo *relinfo_ = nullptr;
};
}  // namespace paxc

namespace pax {
#define TOAST_FILE_SUFFIX ".toast"
class IndexUpdater final {
 public:
  void Begin(Relation rel) {
    CBDB_WRAP_START;
    { stub_.Begin(rel); }
    CBDB_WRAP_END;
  }
  void UpdateIndex(TupleTableSlot *slot) {
    CBDB_WRAP_START;
    { stub_.UpdateIndex(slot); }
    CBDB_WRAP_END;
  }
  void End() {
    CBDB_WRAP_START;
    { stub_.End(); }
    CBDB_WRAP_END;
  }
  inline TupleTableSlot *GetSlot() { return stub_.GetSlot(); }
  inline bool HasIndex() const { return stub_.HasIndex(); }

 private:
  paxc::IndexUpdaterInternal stub_;
};
}  // namespace pax

namespace pax {

TableWriter::TableWriter(Relation relation)
    : relation_(relation), summary_callback_(nullptr) {
  Assert(relation);
  is_dfs_table_space_ =
      relation_->rd_rel->reltablespace != InvalidOid &&
      cbdb::IsDfsTablespaceById(relation_->rd_rel->reltablespace);

  if (is_dfs_table_space_) {
    file_system_options_ =
        PAX_NEW<RemoteFileSystemOptions>(relation_->rd_rel->reltablespace);
    file_system_ = Singleton<RemoteFileSystem>::GetInstance();
  } else {
    file_system_ = Singleton<LocalFileSystem>::GetInstance();
  }
}

PaxStorageFormat TableWriter::GetStorageFormat() {
  if (!already_get_format_) {
    Assert(relation_);
    storage_format_ = StorageFormatKeyToPaxStorageFormat(RelationGetOptions(
        relation_, storage_format, pax_default_storage_format));
    already_get_format_ = true;
  }

  return storage_format_;
}

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

  PAX_DELETE(strategy_);
  PAX_DELETE(mp_stats_);
  PAX_DELETE(file_system_options_);
}

const FileSplitStrategy *TableWriter::GetFileSplitStrategy() const {
  return strategy_;
}

std::string TableWriter::GenFilePath(const std::string &block_id) {
  return cbdb::BuildPaxFilePath(rel_path_, block_id);
}

std::string TableWriter::GenToastFilePath(const std::string &file_path) {
  return file_path + TOAST_FILE_SUFFIX;
}

std::vector<int> TableWriter::GetMinMaxColumnIndexes() {
  if (!already_get_min_max_col_idx_) {
    min_max_col_idx_ = cbdb::GetMinMaxColumnsIndex(relation_);
    already_get_min_max_col_idx_ = true;
  }

  return min_max_col_idx_;
}

std::vector<std::tuple<ColumnEncoding_Kind, int>>
TableWriter::GetRelEncodingOptions() {
  size_t nattrs = 0;
  paxc::PaxOptions **pax_options = nullptr;
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  CBDB_WRAP_START;
  { pax_options = paxc::paxc_relation_get_attribute_options(relation_); }
  CBDB_WRAP_END;
  Assert(pax_options);

  nattrs = relation_->rd_att->natts;

  for (size_t index = 0; index < nattrs; index++) {
    if (pax_options[index]) {
      encoding_opts.emplace_back(std::make_tuple(
          CompressKeyToColumnEncodingKind(pax_options[index]->compress_type),
          pax_options[index]->compress_level));
    } else {
      // TODO(jiaqizho): In pax, we will fill a `DEF_ENCODED` if user not set
      // the encoding clause. Need a GUC to decide whether we should use
      // `NO_ENCODE` or keep use `DEF_ENCODED` also may allow user define
      // different default encoding type for the different pg_type?
      encoding_opts.emplace_back(
          std::make_tuple(ColumnEncoding_Kind_DEF_ENCODED, 0));
    }
  }
  cbdb::Pfree(pax_options);
  return encoding_opts;
}

MicroPartitionWriter *TableWriter::CreateMicroPartitionWriter(
    MicroPartitionStats *mp_stats) {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string toast_file_path;
  std::string block_id;
  File *file;
  File *toast_file = nullptr;
  MicroPartitionWriter *mp_writer;

  Assert(relation_);
  Assert(strategy_);
  Assert(summary_callback_);

  block_id = GenerateBlockID(relation_);
  file_path = GenFilePath(block_id);
  toast_file_path = GenToastFilePath(file_path);
  current_blockno_ = std::stol(block_id);

  options.rel_oid = relation_->rd_id;
  options.rel_tuple_desc = relation_->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);
  options.encoding_opts = std::move(GetRelEncodingOptions());
  options.storage_format = GetStorageFormat();
  options.lengths_encoding_opts = std::make_pair(
      PAX_LENGTHS_DEFAULT_COMPRESSTYPE, PAX_LENGTHS_DEFAULT_COMPRESSLEVEL);
  options.enable_min_max_col_idxs = GetMinMaxColumnIndexes();

  // should be kReadWriteMode here
  // cause PAX may do read after write in partition logic
  file = file_system_->Open(options.file_name, fs::kReadWriteMode,
                            file_system_options_);
  Assert(file);

  if (pax_enable_toast) {
    toast_file = file_system_->Open(toast_file_path, fs::kReadWriteMode,
                                    file_system_options_);
  }

  mp_writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      std::move(options), file, toast_file);

  mp_writer->SetWriteSummaryCallback(summary_callback_)
      ->SetStatsCollector(mp_stats);
  return mp_writer;
}

void TableWriter::Open() {
  rel_path_ = cbdb::BuildPaxDirectoryPath(
      relation_->rd_node, relation_->rd_backend, is_dfs_table_space_);

  // Exception may be thrown causing writer_ to be nullptr

  if (!mp_stats_) {
    mp_stats_ = PAX_NEW<MicroPartitionStats>(RelationGetDescr(relation_));
    mp_stats_->Initialize(GetMinMaxColumnIndexes());
  } else {
    mp_stats_->Reset();
  }

  writer_ = CreateMicroPartitionWriter(mp_stats_);
  num_tuples_ = 0;
  // insert tuple into the aux table before inserting any tuples.
  cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_),
                                        std::to_string(current_blockno_));
}

void TableWriter::WriteTuple(TupleTableSlot *slot) {
  Assert(writer_);
  Assert(strategy_);
  // should check split strategy before write tuple
  // otherwise, may got a empty file in the disk
  if (strategy_->ShouldSplit(writer_->PhysicalSize(), num_tuples_)) {
    writer_->Close();
    PAX_DELETE(writer_);
    Open();
  }

  writer_->WriteTuple(slot);
  SetBlockNumber(&slot->tts_tid, current_blockno_);
  ++num_tuples_;
}

void TableWriter::Close() {
  if (writer_) {
    writer_->Close();
    PAX_DELETE(writer_);
    writer_ = nullptr;
  }
  num_tuples_ = 0;
}

TableReader::TableReader(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    ReaderOptions options)
    : iterator_(std::move(iterator)),
      reader_(nullptr),
      is_empty_(true),
      reader_options_(options) {
  is_dfs_table_space_ =
      cbdb::IsDfsTablespaceById(reader_options_.table_space_id);

  if (is_dfs_table_space_) {
    file_system_ = Singleton<RemoteFileSystem>::GetInstance();
    file_system_options_ =
        PAX_NEW<RemoteFileSystemOptions>(reader_options_.table_space_id);
  } else {
    file_system_ = Singleton<LocalFileSystem>::GetInstance();
  }
}

TableReader::~TableReader() {
  if (reader_) {
    reader_->Close();
    PAX_DELETE(reader_);
    reader_ = nullptr;
  }
  PAX_DELETE(file_system_options_);
}

void TableReader::Open() {
  if (!iterator_->HasNext()) {
    is_empty_ = true;
    return;
  }
  OpenFile();
  is_empty_ = false;
}

void TableReader::ReOpen() {
  Close();
  iterator_->Rewind();
  Open();
}

void TableReader::Close() {
  if (is_empty_) {
    return;
  }

  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

bool TableReader::ReadTuple(TupleTableSlot *slot) {
  if (is_empty_) {
    return false;
  }

  ExecClearTuple(slot);
  SetBlockNumber(&slot->tts_tid, current_block_number_);
  while (!reader_->ReadTuple(slot)) {
    reader_->Close();
    if (!iterator_->HasNext()) {
      is_empty_ = true;
      return false;
    }
    OpenFile();
    SetBlockNumber(&slot->tts_tid, current_block_number_);
  }

  Assert(TTS_EMPTY(slot));
  ExecStoreVirtualTuple(slot);

  return true;
}

bool TableReader::GetTuple(TupleTableSlot *slot, ScanDirection direction,
                           size_t offset) {
  MicroPartitionReader::ReaderOptions options;
  ReaderFlags reader_flags = FLAGS_EMPTY;
  size_t row_index = current_block_row_index_;
  size_t max_row_index;
  size_t remaining_offset = offset;
  File *toast_file = nullptr;
  bool ok;

  Assert(direction == ForwardScanDirection);
  Assert(current_block_metadata_.GetTupleCount() >= 1);
  max_row_index = current_block_metadata_.GetTupleCount() - 1;

  // The number of remaining rows in the current block is enough to satisfy the
  // offset
  if ((max_row_index - row_index) >= remaining_offset) {
    current_block_row_index_ = row_index + remaining_offset;
    SetBlockNumber(&slot->tts_tid,
                   std::stol(current_block_metadata_.GetMicroPartitionId()));
    ok = reader_->GetTuple(slot, current_block_row_index_);
    return ok;
  }

  // There are not enough lines remaining in the current block and we need to
  // jump to the next block
  remaining_offset -= (max_row_index - row_index);

  // iterator_->HasNext() must be true here.
  // When analyzing, pax read the number of tuples in the auxiliary table
  // through relation_estimate_size. The number of tuples is a correct number.
  // The target_tuple_id (the return blocknumber of BlockSampler_Next ) sampled
  // by analyze will not exceed the number of tuples.
  CBDB_CHECK(
      iterator_->HasNext(), cbdb::CException::kExTypeOutOfRange,
      fmt("No more tuples to read. Invalid [target offsets=%lu, remain "
          "offsets=%lu]. ",
          offset, remaining_offset - current_block_metadata_.GetTupleCount()));

  while (iterator_->HasNext()) {
    current_block_metadata_ = iterator_->Next();
    if (current_block_metadata_.GetTupleCount() >= remaining_offset) {
      break;
    }
    remaining_offset -= current_block_metadata_.GetTupleCount();
  }

  // remain_offset must point to an existing tuple (whether it is valid or
  // invalid)
  CBDB_CHECK(current_block_metadata_.GetTupleCount() >= remaining_offset,
             cbdb::CException::kExTypeLogicError,
             fmt("Invalid tuple counts in current block [block tuple counts= "
                 "%ul, remain offsets=%lu]\n"
                 "Meta data [name=%s, id=%s]. ",
                 current_block_metadata_.GetTupleCount(), remaining_offset,
                 current_block_metadata_.GetFileName().c_str(),
                 current_block_metadata_.GetMicroPartitionId().c_str()));

  // close old reader
  if (reader_) {
    reader_->Close();
    PAX_DELETE(reader_);
  }

  options.block_id = current_block_metadata_.GetMicroPartitionId();
  options.filter = reader_options_.filter;
  options.reused_buffer = reader_options_.reused_buffer;

  // load visibility map
  // TODO(jiaqizho): PAX should not do read/pread in table layer
  std::string visibility_bitmap_file =
      current_block_metadata_.GetVisibilityBitmapFile();
  if (!visibility_bitmap_file.empty()) {
    auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode,
                                   file_system_options_);
    auto file_length = file->FileLength();
    auto bm = std::make_shared<Bitmap8>(file_length * 8);
    file->ReadN(bm->Raw().bitmap, file_length);
    options.visibility_bitmap = bm;
    file->Close();
  }

  if (file_system_->Exist(
          current_block_metadata_.GetFileName() + TOAST_FILE_SUFFIX,
          file_system_options_)) {
    toast_file = file_system_->Open(
        current_block_metadata_.GetFileName() + TOAST_FILE_SUFFIX,
        fs::kReadMode, file_system_options_);
  }

  reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
      std::move(options), reader_flags,
      file_system_->Open(current_block_metadata_.GetFileName(), fs::kReadMode,
                         file_system_options_),
      toast_file);

  // row_index start from 0, so row_index = offset -1
  current_block_row_index_ = remaining_offset - 1;
  SetBlockNumber(&slot->tts_tid,
                 std::stol(current_block_metadata_.GetMicroPartitionId()));

  ok = reader_->GetTuple(slot, current_block_row_index_);
  return ok;
}

void TableReader::OpenFile() {
  Assert(iterator_->HasNext());
  auto it = iterator_->Next();
  current_block_metadata_ = it;
  MicroPartitionReader::ReaderOptions options;
  File *toast_file = nullptr;
  int32 reader_flags = FLAGS_EMPTY;

  micro_partition_id_ = options.block_id = it.GetMicroPartitionId();
  current_block_number_ = std::stol(options.block_id);

  options.filter = reader_options_.filter;
  options.reused_buffer = reader_options_.reused_buffer;

  PAX_DELETE(reader_);

  // load visibility map
  std::string visibility_bitmap_file = it.GetVisibilityBitmapFile();
  if (!visibility_bitmap_file.empty()) {
    auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode,
                                   file_system_options_);
    auto file_length = file->FileLength();
    auto bm = std::make_shared<Bitmap8>(file_length * 8);
    file->ReadN(bm->Raw().bitmap, file_length);
    options.visibility_bitmap = bm;
    file->Close();
  }
#ifdef VEC_BUILD
  options.tuple_desc = reader_options_.tuple_desc;
  if (reader_options_.is_vec) {
    Assert(options.tuple_desc);
    READER_FLAG_SET_VECTOR_PATH(reader_flags);
  }

  if (reader_options_.vec_build_ctid)
    READER_FLAG_SET_SCAN_WITH_CTID(reader_flags);
#endif
  if (file_system_->Exist(it.GetFileName() + TOAST_FILE_SUFFIX,
                          file_system_options_)) {
    toast_file = file_system_->Open(it.GetFileName() + TOAST_FILE_SUFFIX,
                                    fs::kReadMode, file_system_options_);
  }

  reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
      std::move(options), reader_flags,
      file_system_->Open(it.GetFileName(), fs::kReadMode, file_system_options_),
      toast_file);
}

TableDeleter::TableDeleter(
    Relation rel,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    std::map<std::string, std::shared_ptr<Bitmap8>> delete_bitmap,
    Snapshot snapshot)
    : rel_(rel),
      iterator_(std::move(iterator)),
      delete_bitmap_(delete_bitmap),
      snapshot_(snapshot),
      reader_(nullptr),
      writer_(nullptr) {
  if (cbdb::IsDfsTablespaceById(rel_->rd_rel->reltablespace)) {
    file_system_options_ =
        PAX_NEW<RemoteFileSystemOptions>(rel_->rd_rel->reltablespace);
    file_system_ = Singleton<RemoteFileSystem>::GetInstance();
  } else {
    file_system_ = Singleton<LocalFileSystem>::GetInstance();
  }
}

TableDeleter::~TableDeleter() {
  if (reader_) {
    reader_->Close();
    PAX_DELETE(reader_);
    reader_ = nullptr;
  }

  if (writer_) {
    writer_->Close();
    PAX_DELETE(writer_);
    writer_ = nullptr;
  }

  PAX_DELETE(file_system_options_);
}

// The pattern of file name of the visimap file is:
// <blocknum>_<generation>_<tag>.visimap
// blocknum: the corresponding micro partition that the visimap is for
// generation: every delete operation will increase the generation
// tag: used to make the file name unique in reality. Only the first two
//      parts is enough to be unique in normal. If the next generation
//      visimap file is generated, but was canceled later, the visimap
//      file may be not deleted in time. Then, the next delete will
//      use the same file name for visimap file.
void TableDeleter::DeleteWithVisibilityMap(TransactionId delete_xid) {
  if (!iterator_->HasNext()) {
    return;
  }

  Bitmap8 *visi_bitmap = nullptr;
  auto aux_oid = cbdb::GetPaxAuxRelid(RelationGetRelid(rel_));
  auto rel_path = cbdb::BuildPaxDirectoryPath(
      rel_->rd_node, rel_->rd_backend,
      cbdb::IsDfsTablespaceById(rel_->rd_rel->reltablespace));

  do {
    auto it = iterator_->Next();

    auto block_id = it.GetMicroPartitionId();
    auto micro_partition_metadata =
        cbdb::GetMicroPartitionMetadata(rel_, snapshot_, block_id);
    int generate = 0;
    char visimap_file_name[128];

    // union bitmap
    auto delete_visi_bitmap = delete_bitmap_[block_id]->Clone();
    // read visibility map
    {
      int rc pg_attribute_unused();
      auto visibility_map_filename =
          micro_partition_metadata.GetVisibilityBitmapFile();

      if (!visibility_map_filename.empty()) {
        int blocknum;
        TransactionId xid;

        std::string v_file_name =
            cbdb::BuildPaxFilePath(rel_path, visibility_map_filename);
        auto buffer =
            LoadVisimap(file_system_, file_system_options_, v_file_name);
        auto visibility_file_bitmap =
            Bitmap8(BitmapRaw<uint8>(buffer->data(), buffer->size()),
                    Bitmap8::ReadOnlyOwnBitmap);
        visi_bitmap =
            Bitmap8::Union(&visibility_file_bitmap, delete_visi_bitmap);

        PAX_DELETE<Bitmap8>(delete_visi_bitmap);

        rc = sscanf(visibility_map_filename.c_str(), "%d_%x_%x.visimap",
                    &blocknum, &generate, &xid);
        Assert(blocknum >= 0 && block_id == std::to_string(blocknum));
        (void)xid;
        CBDB_CHECK(rc == 3, cbdb::CException::kExTypeLogicError,
                   fmt("Fail to sscanf [rc=%d, filename=%s, rel_path=%s]", rc,
                       visibility_map_filename.c_str(), rel_path.c_str()));
      } else {
        visi_bitmap = delete_visi_bitmap;
      }

      // generate new file name for visimap
      rc = snprintf(visimap_file_name, sizeof(visimap_file_name),
                    "%s_%x_%x.visimap", block_id.c_str(), generate + 1,
                    delete_xid);
      Assert(rc <= NAMEDATALEN);
    }

    Assert(visi_bitmap != nullptr);
    std::string visimap_file_path =
        cbdb::BuildPaxFilePath(rel_path.c_str(), visimap_file_name);
    {
      auto &raw = visi_bitmap->Raw();
      auto visimap_file = file_system_->Open(visimap_file_path, fs::kWriteMode,
                                             file_system_options_);
      visimap_file->WriteN(raw.bitmap, raw.size);
      visimap_file->Close();
    }
    // Replace PAX_DELETE by smart pointer
    PAX_DELETE<Bitmap8>(visi_bitmap);
    visi_bitmap = nullptr;

    // write pg_pax_blocks_oid
    cbdb::UpdateVisimap(aux_oid, block_id.c_str(), visimap_file_name);
  } while (iterator_->HasNext());
}

void TableDeleter::Delete() {
  if (!iterator_->HasNext()) {
    return;
  }
  OpenReader();
  OpenWriter();
  pax::IndexUpdater index_updater;

  index_updater.Begin(rel_);
  Assert(rel_->rd_rel->relhasindex == index_updater.HasIndex());
  auto slot = index_updater.GetSlot();

  slot->tts_tableOid = RelationGetRelid(rel_);
  // TODO(gongxun): because bulk insert as AO/HEAP does with tuples iteration
  // not implemented. we should implement bulk insert firstly. and then we can
  // use ReadTupleN and WriteTupleN to delete tuples in batch.
  while (reader_->ReadTuple(slot)) {
    auto block_id = reader_->GetCurrentMicroPartitionId();
    auto it = delete_bitmap_.find(block_id);
    Assert(it != delete_bitmap_.end());

    auto bitmap = it->second.get();
    if (bitmap->Test(pax::GetTupleOffset(slot->tts_tid))) continue;

    writer_->WriteTuple(slot);
    if (index_updater.HasIndex()) {
      // Already store the ctid after WriteTuple
      Assert(!TTS_EMPTY(slot));
      Assert(ItemPointerIsValid(&slot->tts_tid));
      index_updater.UpdateIndex(slot);
    }
  }
  index_updater.End();
}

void TableDeleter::OpenWriter() {
  writer_ = PAX_NEW<TableWriter>(rel_);
  writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(PAX_NEW<PaxDefaultSplitStrategy>())
      ->Open();
}

void TableDeleter::OpenReader() {
  TableReader::ReaderOptions reader_options{};
  reader_options.table_space_id = rel_->rd_rel->reltablespace;
  reader_ = PAX_NEW<TableReader>(std::move(iterator_), reader_options);
  reader_->Open();
}

}  // namespace pax
