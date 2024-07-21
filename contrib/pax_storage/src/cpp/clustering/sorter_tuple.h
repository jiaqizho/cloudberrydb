#pragma once

#include "clustering/sorter.h"

namespace pax {
namespace clustering {
class TupleSorter final : public Sorter {
 public:
  struct HeapTupleSorterOptions {
    TupleSorterOptions base;
    TupleDesc tup_desc;
    AttrNumber *attr;
    int nkeys;
    Oid sortOperators;
    Oid sortCollations;
    bool nulls_first_flags;
    int work_mem;
    HeapTupleSorterOptions() { base.type = kTupleSorterTypeHeap; }
  };

 public:
  TupleSorter(HeapTupleSorterOptions options);
  virtual ~TupleSorter();
  virtual void AppendSortData(TupleTableSlot *slot) override;
  virtual void Sort() override;
  virtual bool GetSortedData(TupleTableSlot *slot) override;

 private:
  void Init();
  void DeInit();

  HeapTupleSorterOptions options_;
  Tuplesortstate *sort_state_;
};
}  // namespace clustering

}  // namespace pax