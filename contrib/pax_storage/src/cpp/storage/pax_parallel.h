#pragma once

#include <shared_mutex>
#include <thread>
#include <unordered_map>

#ifdef VEC_BUILD
#include "comm/cbdb_api.h"

#include "storage/filter/pax_filter.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"
#include "storage/pax.h"
#include "storage/pax_itemptr.h"
#include "storage/vec/arrow_wrapper.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace pax {

extern std::shared_ptr<arrow::Schema> TupdescToArrowSchema(TupleDesc tupdesc);

extern std::vector<int> SchemaToIndex(TupleDesc desc, arrow::Schema *schema);

template <typename T>
class ParallelIterator {
 public:
  virtual ~ParallelIterator() = default;
  virtual std::optional<T> Next() = 0;
};

class PaxFragmentInterface;

class ParallelScanDesc : public std::enable_shared_from_this<ParallelScanDesc> {
 public:
  inline bool ShouldBuildCtid() const { return build_ctid_bitmap_; }
  inline ParallelIterator<MicroPartitionMetadata> *Iterator() {
    return iterator_.get();
  }
  ~ParallelScanDesc() = default;

  arrow::Status Initialize(
      uint32_t tableoid, const std::shared_ptr<arrow::Schema> &table_schema,
      const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options);
  void Release();
  const std::shared_ptr<arrow::Schema> &TableSchema() const {
    return table_schema_;
  }
  std::shared_ptr<arrow::Schema> &TableSchema() { return table_schema_; }
  const std::shared_ptr<arrow::Schema> &ScanSchema() const {
    return scan_schema_;
  }
  std::shared_ptr<arrow::Schema> &ScanSchema() { return scan_schema_; }
  const std::vector<int> &ScanColumns() const { return scan_columns_; }
  const std::shared_ptr<PaxFilter> &GetPaxFilter() const { return pax_filter_; }
  Relation GetRelation() const { return relation_; }

 private:
  void CalculateScanColumns(
      const std::vector<std::pair<const char *, size_t>> &table_names);
  void TransformFilterExpression(
      const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options,
      const arrow::compute::Expression &expr,
      const std::vector<std::pair<const char *, size_t>> &table_names);

  struct FragmentIteratorInternal {
    FragmentIteratorInternal(const std::shared_ptr<ParallelScanDesc> &desc);
    FragmentIteratorInternal(const FragmentIteratorInternal &copy) = default;
    arrow::Result<std::shared_ptr<arrow::dataset::Fragment>> Next();

    std::shared_ptr<ParallelScanDesc> desc_;
    int fragment_counter_;
  };
  Relation relation_ = nullptr;
  std::shared_ptr<PaxFilter> pax_filter_;
  std::vector<int> scan_columns_;
  std::unique_ptr<ParallelIterator<MicroPartitionMetadata>> iterator_;
  std::shared_ptr<arrow::Schema> table_schema_;
  std::shared_ptr<arrow::Schema> scan_schema_;
  int num_micro_partitions_ = 0;
  bool build_ctid_bitmap_ = false;
  bool has_called_ = false;  // has called GetPaxFragmentGen()

  friend class PaxDatasetInterface;
};

class PaxDatasetInterface : public arrow::dataset::DatasetInterface {
 public:
  // Initialize should be able to call postgres functions, if error happens,
  // will raise the PG ERROR
  PaxDatasetInterface(std::shared_ptr<arrow::Schema> table_schema)
      : DatasetInterface(table_schema) {}
  arrow::Status Initialize(uint32_t tableoid,
                           const std::shared_ptr<arrow::dataset::ScanOptions>
                               &scan_options) override {
    desc_ = std::make_shared<ParallelScanDesc>();
    return desc_->Initialize(tableoid, schema(), scan_options);
  }

  void Release() override {
    if (desc_) desc_->Release();
  }
  std::string type_name() const override { return "pax-parallel-scan"; }
  arrow::Result<arrow::dataset::FragmentIterator> GetFragmentsImpl(
      arrow::compute::Expression predicate) override;

  static std::shared_ptr<arrow::dataset::DatasetInterface> New(
      std::shared_ptr<arrow::Schema> table_schema) {
    return std::make_shared<PaxDatasetInterface>(table_schema);
  }

 private:
  std::shared_ptr<ParallelScanDesc> desc_;
};

class PaxFragmentInterface : public arrow::dataset::FragmentInterface {
 public:
  PaxFragmentInterface(const std::shared_ptr<ParallelScanDesc> &scan_desc);
  virtual ~PaxFragmentInterface() = default;
  arrow::Result<arrow::RecordBatchIterator> ScanBatchesAsyncImpl(
      const std::shared_ptr<arrow::dataset::ScanOptions> &options) override;
  std::string type_name() const override { return "pax-fragment"; }
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next();

  int BlockNum() const { return block_no_; }
  const std::shared_ptr<arrow::Schema> &ScanSchema() const {
    return scan_desc_->ScanSchema();
  }
  const std::vector<int> &ScanColumns() const {
    return scan_desc_->ScanColumns();
  }
  void Release() override;

 private:
  bool OpenFile();
  void InitAdapter();
  void CalculateScanColumns();

  // readonly reference
  Relation relation_ = nullptr;

  // owned adapter
  std::shared_ptr<VecAdapter> adapter_;

  // owned reader
  std::unique_ptr<PaxVecReader> reader_;

  std::shared_ptr<std::vector<uint8_t>> visimap_;
  // readonly reference
  std::shared_ptr<ParallelScanDesc> scan_desc_;
  int block_no_ = -1;
};

}  // namespace pax

#endif