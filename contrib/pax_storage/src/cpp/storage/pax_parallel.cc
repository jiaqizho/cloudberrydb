#include "storage/pax_parallel.h"

#include <algorithm>
#include <stdexcept>

#include "access/pax_visimap.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pg_pax_tables.h"
#include "comm/cbdb_wrappers.h"
#include "comm/paxc_wrappers.h"
#include "comm/vec_numeric.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"
#include "storage/filter/pax_sparse_filter.h"
#ifdef VEC_BUILD

namespace pax {

class PaxRecordBatchGenerator {
 public:
  PaxRecordBatchGenerator(const std::shared_ptr<PaxFragmentInterface> &desc)
      : desc_(desc) {}
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    return desc_->Next();
  }

 private:
  std::shared_ptr<PaxFragmentInterface> desc_;
};

std::vector<int> SchemaToIndex(TupleDesc desc, arrow::Schema *schema) {
  std::vector<int> result;
  auto names = schema->field_names();
  for (int i = 0; i < desc->natts; i++) {
    auto attr = TupleDescAttr(desc, i);
    char *attname;

    if (attr->attisdropped) continue;

    attname = NameStr(attr->attname);
    if (std::find(names.begin(), names.end(), attname) != names.end()) {
      result.push_back(i);
    }
  }

  if (std::find(names.begin(), names.end(), "ctid") != names.end()) {
    result.push_back(SelfItemPointerAttributeNumber);
  }
  if (result.size() == names.size()) return result;
  throw std::logic_error("unknown name in schema");
}

template <typename T>
class ParallelIteratorImpl : public ParallelIterator<T> {
 public:
  ParallelIteratorImpl(std::vector<T> &&v) : v_(std::move(v)), index_(0) {}
  virtual ~ParallelIteratorImpl() = default;
  std::optional<T> Next() override;

 private:
  std::vector<T> v_;
  std::atomic_int32_t index_;
};

template <typename T>
std::optional<T> ParallelIteratorImpl<T>::Next() {
  int32_t index;
  int32_t next;
  auto size = static_cast<int32_t>(v_.size());

  index = index_.load(std::memory_order_relaxed);
  do {
    next = index + 1;
  } while (index < size && !index_.compare_exchange_weak(
                               index, next, std::memory_order_relaxed));

  if (index >= size) return std::optional<T>();
  return std::optional<T>(v_[index]);
}

PaxFragmentInterface::PaxFragmentInterface(
    const std::shared_ptr<ParallelScanDesc> &scan_desc)
    : scan_desc_(scan_desc), block_no_(-1) {
  Assert(scan_desc && scan_desc->GetRelation());

  relation_ = scan_desc->GetRelation();
}

void PaxFragmentInterface::Release() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

void PaxFragmentInterface::InitAdapter() {
  if (adapter_)
    adapter_->Reset();
  else
    adapter_ = std::make_shared<VecAdapter>(RelationGetDescr(relation_), 0,
                                            scan_desc_->ShouldBuildCtid());
}

// return true if open micro partition successfully, false if no more micro
// partition left
bool PaxFragmentInterface::OpenFile() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
    visimap_ = nullptr;
  }

  auto op = scan_desc_->Iterator()->Next();
  if (!op) return false;

  auto file_system = Singleton<LocalFileSystem>::GetInstance();
  auto m = std::move(op).value();
  auto filter = scan_desc_->GetPaxFilter();

  MicroPartitionReader::ReaderOptions options;
  block_no_ = m.GetMicroPartitionId();

  // TODO: convert arrow filter to pax filter
  options.filter = filter;
  options.reused_buffer = nullptr;
  {
    const auto &visimap_name = m.GetVisibilityBitmapFile();
    if (!visimap_name.empty()) {
      visimap_ = pax::LoadVisimap(file_system, nullptr, visimap_name);
      BitmapRaw<uint8_t> raw(visimap_->data(), visimap_->size());
      options.visibility_bitmap =
          std::make_shared<Bitmap8>(raw, BitmapTpl<uint8>::ReadOnlyRefBitmap);
    }
  }

  InitAdapter();

  auto data_file = file_system->Open(m.GetFileName(), fs::kReadMode);
  std::shared_ptr<File> toast_file;
  if (m.GetExistToast()) {
    toast_file =
        file_system->Open(m.GetFileName() + TOAST_FILE_SUFFIX, fs::kReadMode);
  }
  auto reader = std::make_unique<OrcReader>(std::move(data_file), 
    std::move(toast_file));


  reader_ = std::make_unique<PaxVecReader>(std::move(reader), adapter_, filter);
  reader_->Open(options);
  return true;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
PaxFragmentInterface::Next() {
  std::shared_ptr<arrow::RecordBatch> result;

  if (reader_) result = reader_->ReadBatch(this);

  while (!result) {
    // If no more tuples in the current reader, try next micro partition
    if (!OpenFile())
      // No micro partitions left
      return arrow::IterationTraits<std::shared_ptr<arrow::RecordBatch>>::End();

    // If the current micro partition has no tuple, try next one
    result = reader_->ReadBatch(this);
  }
  return result;
}

arrow::Result<arrow::RecordBatchIterator>
PaxFragmentInterface::ScanBatchesAsyncImpl(
    const std::shared_ptr<arrow::dataset::ScanOptions> &options) {
  PaxRecordBatchGenerator g(
      std::dynamic_pointer_cast<PaxFragmentInterface>(shared_from_this()));

  Assert(scan_desc_->ScanSchema()->Equals(options->projected_schema));

  return arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>(std::move(g));
}

void ParallelScanDesc::CalculateScanColumns(
    const std::vector<std::pair<const char *, size_t>> &table_names) {
  Assert(scan_columns_.empty());

  auto natts = RelationGetNumberOfAttributes(relation_);
  std::vector<bool> proj_bits(natts, false);

  build_ctid_bitmap_ = false;
  for (int i = 0, n = scan_schema_->num_fields(); i < n; i++) {
    const auto &name = scan_schema_->field(i)->name();
    auto pname = arrow::ExtractFieldName(name);
    auto index = arrow::FindFieldIndex(table_names, pname);

    if (index >= 0) {
      Assert(!proj_bits[index]);  // only once
      scan_columns_.push_back(index);
      proj_bits[index] = true;
    } else {
      Assert(index == SelfItemPointerAttributeNumber);
      Assert(!build_ctid_bitmap_);  // only once

      build_ctid_bitmap_ = true;
      scan_columns_.push_back(SelfItemPointerAttributeNumber);
    }
  }

  Assert(pax_filter_);
  pax_filter_->SetColumnProjection(std::move(proj_bits));
}

arrow::Status ParallelScanDesc::Initialize(
    uint32_t tableoid, const std::shared_ptr<arrow::Schema> &table_schema,
    const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options) {
  relation_ = cbdb::TableOpen(tableoid, AccessShareLock);
  auto tupdesc = RelationGetDescr(relation_);

  std::vector<MicroPartitionMetadata> result;

  table_schema_ = table_schema;
  scan_schema_ = scan_options->dataset_schema;
  pax_filter_ = std::make_shared<PaxFilter>();

  std::vector<std::pair<const char *, size_t>> table_names;
  for (int i = 0, n = table_schema_->num_fields(); i < n; i++)
    table_names.push_back(
        arrow::ExtractFieldName(table_schema_->field(i)->name()));

  CalculateScanColumns(table_names);
  pax_filter_->InitSparseFilter(relation_, scan_options->filter, table_names);

  auto it = MicroPartitionInfoIterator::New(relation_, nullptr);
  while (it->HasNext()) {
    auto meta = it->Next();
    bool ok = true;
    if (pax_filter_->SparseFilterEnabled()) {
      MicroPartitionStatsProvider provider(meta.GetStats());
      ok = pax_filter_->ExecSparseFilter(provider, tupdesc,
                                         PaxSparseFilter::StatisticsKind::kFile);
    }
    if (ok) result.emplace_back(std::move(meta));
  }
  it->Release();

  num_micro_partitions_ = static_cast<int>(result.size());
  iterator_ = std::make_unique<ParallelIteratorImpl<MicroPartitionMetadata>>(
      std::move(result));

  return arrow::Status::OK();
}

void ParallelScanDesc::Release() {
  Assert(relation_);

  if (pax_enable_debug && pax_filter_) {
    pax_filter_->LogStatistics();
  }
  cbdb::TableClose(relation_, NoLock);
  relation_ = nullptr;
  iterator_ = nullptr;
}

ParallelScanDesc::FragmentIteratorInternal::FragmentIteratorInternal(
    const std::shared_ptr<ParallelScanDesc> &desc)
    : desc_(desc), fragment_counter_(0) {}

arrow::Result<std::shared_ptr<arrow::dataset::Fragment>>
ParallelScanDesc::FragmentIteratorInternal::Next() {
  // limit the number of fragment threads.
  if (fragment_counter_ >= desc_->num_micro_partitions_)
    return arrow::IterationTraits<
        std::shared_ptr<arrow::dataset::Fragment>>::End();

  fragment_counter_++;
  return std::static_pointer_cast<arrow::dataset::Fragment>(
      std::make_shared<PaxFragmentInterface>(desc_));
}

arrow::Result<arrow::dataset::FragmentIterator>
PaxDatasetInterface::GetFragmentsImpl(arrow::compute::Expression predicate) {
  ParallelScanDesc::FragmentIteratorInternal it(desc_);
  return arrow::Iterator<std::shared_ptr<arrow::dataset::Fragment>>(
      std::move(it));
}

}  // namespace pax

#endif
