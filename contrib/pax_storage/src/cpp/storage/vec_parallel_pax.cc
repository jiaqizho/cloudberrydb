#include "storage/vec_parallel_pax.h"

#ifdef VEC_BUILD
#include "access/pax_visimap.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {

class MicroPartitionInfo : public MicroPartitionInfoProvider {
 public:
  MicroPartitionInfo(MicroPartitionMetadata &&md): md_(std::move(md)) { }
  MicroPartitionInfo(const MicroPartitionMetadata &md) = delete;
  MicroPartitionInfo(MicroPartitionInfo &&md): md_(std::move(md.md_)) { }
  MicroPartitionInfo(const MicroPartitionInfo &md) = delete;

  std::string GetFileName() const override { return md_.GetFileName(); }
  std::string GetToastName() const override {
    if (md_.GetExistToast())
      return md_.GetFileName() + TOAST_FILE_SUFFIX;
    return "";
  }
  int GetBlockId() const override {
    return md_.GetMicroPartitionId();
  }

  std::pair<std::shared_ptr<std::vector<uint8_t>>, std::unique_ptr<Bitmap8>>
  GetVisibilityBitmap(FileSystem *file_system) override {
    std::shared_ptr<std::vector<uint8_t>> visimap;
    std::unique_ptr<Bitmap8> bitmap;
    const auto &visimap_name = md_.GetVisibilityBitmapFile();
    if (!visimap_name.empty()) {
      visimap = pax::LoadVisimap(file_system, nullptr, visimap_name);
      BitmapRaw<uint8_t> raw(visimap->data(), visimap->size());
      bitmap = std::make_unique<Bitmap8>(raw, BitmapTpl<uint8>::ReadOnlyRefBitmap);
    }
    return {std::move(visimap), std::move(bitmap)};
  }

  const ::pax::stats::MicroPartitionStatisticsInfo &GetStats() const {
    return md_.GetStats();
  }

  MicroPartitionInfo &operator=(MicroPartitionInfo &&other) {
    if (this != &other)
      md_ = std::move(other.md_);
    return *this;
  }
  MicroPartitionInfo &operator=(const MicroPartitionInfo &other) = delete;

 private:
  MicroPartitionMetadata md_;
};

arrow::Status PaxDatasetInterface::Initialize(uint32_t tableoid, const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options) {
  relation_ = cbdb::TableOpen(tableoid, AccessShareLock);
  desc_ = std::make_shared<ParallelScanDesc>();
  auto fs = Singleton<LocalFileSystem>::GetInstance();
  auto it = MicroPartitionInfoIterator::New(relation_, nullptr);

  // TODO: refactor the iterator to support non-copy access,
  //       so unique_ptr can be used here.
  std::vector<std::shared_ptr<MicroPartitionInfoProvider>> v;

  while(it->HasNext()) {
    auto x = std::make_unique<MicroPartitionInfo>(it->Next());
    v.emplace_back(std::move(x));
  }
  it->Release();
  return desc_->Initialize(relation_, schema(), scan_options, fs, nullptr, std::move(VectorIterator(std::move(v))));
}

void PaxDatasetInterface::Release() {
  if (desc_) desc_->Release();
  cbdb::TableClose(relation_, NoLock);
}

arrow::Result<arrow::dataset::FragmentIterator> PaxDatasetInterface::GetFragmentsImpl(arrow::compute::Expression predicate) {
  ParallelScanDesc::FragmentIteratorInternal it(desc_);
  return arrow::Iterator<std::shared_ptr<arrow::dataset::Fragment>>(std::move(it));
}

}
#endif
