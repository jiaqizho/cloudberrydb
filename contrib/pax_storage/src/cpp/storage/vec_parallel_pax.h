#pragma once
#include "storage/vec_parallel_common.h"

namespace pax
{

class PaxDatasetInterface : public arrow::dataset::DatasetInterface {
 public:
  // Initialize should be able to call postgres functions, if error happens, will raise
  // the PG ERROR
  PaxDatasetInterface(std::shared_ptr<arrow::Schema> table_schema): DatasetInterface(table_schema) {}
  arrow::Status Initialize(uint32_t tableoid, const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options) override;

  void Release() override;
  std::string type_name() const override { return "pax-parallel-scan"; }
  arrow::Result<arrow::dataset::FragmentIterator> GetFragmentsImpl(arrow::compute::Expression predicate) override;


  static std::shared_ptr<arrow::dataset::DatasetInterface> New(std::shared_ptr<arrow::Schema> table_schema) {
    return std::make_shared<PaxDatasetInterface>(table_schema);
  }

 private:
  Relation relation_ = nullptr;
  std::shared_ptr<ParallelScanDesc> desc_;
};

} // namespace pax
