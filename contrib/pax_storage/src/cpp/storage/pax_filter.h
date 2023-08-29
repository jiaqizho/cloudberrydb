#pragma once
#include "comm/cbdb_api.h"

#include <utility>

namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
}
bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                   ScanKey *scan_keys, int *num_scan_keys);

class PaxFilter final {
 public:
  PaxFilter() = default;

  ~PaxFilter();

  bool HasMicroPartitionFilter() const { return num_scan_keys_ > 0; }

  std::pair<bool *, size_t> GetColumnProjection();

  void SetColumnProjection(bool *proj, size_t proj_len);

  void SetScanKeys(ScanKey scan_keys, int num_scan_keys);

  // true: if failed to filter the whole micro-partition, reader SHOULD scan the
  // tuples false: if success to filter the micro-partition, the whole
  // micro-partition SHOULD be ignored.
  inline bool TestMicroPartitionScan(
      const pax::stats::MicroPartitionStatisticsInfo &stats,
      TupleDesc desc) const {
    if (num_scan_keys_ == 0) return true;
    return TestMicroPartitionScanInternal(stats, desc);
  }

 private:
  bool TestMicroPartitionScanInternal(
      const pax::stats::MicroPartitionStatisticsInfo &stats,
      TupleDesc desc) const;

  // micro partition filter: we use the scan keys to filter a whole of micro
  // partition by comparing the scan keys with the min/max values in micro
  // partition stats. The memory of the scan keys is allocated by alloc.
  // PaxFilter assumes it only references them.
  ScanKey scan_keys_ = nullptr;
  int num_scan_keys_ = 0;

  // column projection
  bool *proj_ = nullptr;
  size_t proj_len_ = 0;
};  // class PaxFilter

}  // namespace pax
