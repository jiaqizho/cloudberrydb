#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/filter/pax_column_stats.h"

#ifdef VEC_BUILD
#include "storage/vec/arrow_wrapper.h"
#endif

namespace pax {
class PaxSparseFilter;
class PaxRowFilter;
class ColumnStatsProvider;

class PaxFilter final {
 public:
  PaxFilter();
  ~PaxFilter() = default;

  // The sparse filter
  void InitSparseFilter(Relation relation, List *quals,
                        bool allow_fallback_to_pg = false);
#ifdef VEC_BUILD
  void InitSparseFilter(
      Relation relation, const arrow::compute::Expression &expr,
      const std::vector<std::pair<const char *, size_t>> &table_names);
#endif

  bool SparseFilterEnabled();
  bool ExecSparseFilter(const ColumnStatsProvider &provider,
                        const TupleDesc desc, int kind);

  // The projection
  inline const std::vector<bool> &GetColumnProjection() const { return proj_; }
  inline const std::vector<int> &GetColumnProjectionIndex() const {
    return proj_column_index_;
  }

  void SetColumnProjection(std::vector<bool> &&proj_cols);
  void SetColumnProjection(const std::vector<int> &cols, int natts);

  // The row filter
  void InitRowFilter(Relation relation, PlanState *ps,
                     const std::vector<bool> &projection);
  std::shared_ptr<PaxRowFilter> GetRowFilter();

  void LogStatistics() const;

 private:
  std::shared_ptr<PaxSparseFilter> sparse_filter_;
  std::shared_ptr<PaxRowFilter> row_filter_;

  // projection
  std::vector<bool> proj_;
  std::vector<int> proj_column_index_;
};  // class PaxFilter

}  // namespace pax