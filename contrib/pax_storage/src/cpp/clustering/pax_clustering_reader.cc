#include "clustering/pax_clustering_reader.h"

#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/remote_file_system.h"

namespace pax {
namespace clustering {
PaxClusteringReader::PaxClusteringReader(
    Relation relation,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator)
    : relation_(relation), iter_(std::move(iterator)) {
  bool is_dfs_table_space_ =
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

PaxClusteringReader::~PaxClusteringReader() {}

bool PaxClusteringReader::GetNextTuple(TupleTableSlot *slot) {
  ExecClearTuple(slot);
  while (reader_ == nullptr || !reader_->ReadTuple(slot)) {
    if (iter_->HasNext()) {
      if (reader_ != nullptr) {
        reader_->Close();
        PAX_DELETE(reader_);
        reader_ = nullptr;
      }
      auto meta_info = iter_->Next();
      MicroPartitionReader::ReaderOptions options;

      std::string visibility_bitmap_file = meta_info.GetVisibilityBitmapFile();
      if (!visibility_bitmap_file.empty()) {
        auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode,
                                       file_system_options_);
        auto file_length = file->FileLength();
        auto bm = std::make_shared<Bitmap8>(file_length * 8);
        file->ReadN(bm->Raw().bitmap, file_length);
        options.visibility_bitmap = bm;
        file->Close();
      }

      File *file = Singleton<LocalFileSystem>::GetInstance()->Open(
          meta_info.GetFileName(), pax::fs::kReadMode);

      File *toast_file = nullptr;
      if (meta_info.GetExistToast()) {
        toast_file = Singleton<LocalFileSystem>::GetInstance()->Open(
            meta_info.GetFileName() + TOAST_FILE_SUFFIX, pax::fs::kReadMode);
        ;
      }

      reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
          options, ReaderFlags::FLAGS_EMPTY, file, toast_file);
    } else {
      return false;
    }
  }
  ExecStoreVirtualTuple(slot);
  return true;
}

void PaxClusteringReader::Close() {
  if (reader_) {
    reader_->Close();
    PAX_DELETE(reader_);
    reader_ = nullptr;
  }
}

}  // namespace clustering

}  // namespace pax
