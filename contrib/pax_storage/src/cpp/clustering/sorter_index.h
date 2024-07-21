#pragma once

#include "clustering/sorter.h"

namespace pax {
namespace clustering {
class IndexSorter final : public Sorter {
 public:
  struct IndexTupleSorterOptions : public TupleSorterOptions {
    IndexTupleSorterOptions() { type = kTupleSorterTypeIndex; }
    TupleDesc tup_desc;
    Relation index_rel;
    int work_mem;
  };

 public:
  IndexSorter(IndexTupleSorterOptions options);
  virtual ~IndexSorter();
  virtual void AppendSortData(TupleTableSlot *slot) override;
  virtual void Sort() override;
  virtual bool GetSortedData(TupleTableSlot *slot) override;

 private:
  void Init();
  void DeInit();

  IndexTupleSorterOptions options_;
  Tuplesortstate *sort_state_ = nullptr;
};
}  // namespace clustering

}  // namespace pax
