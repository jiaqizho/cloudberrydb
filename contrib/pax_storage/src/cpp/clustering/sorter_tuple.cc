
#include "clustering/sorter_tuple.h"

#include "comm/cbdb_wrappers.h"

namespace pax {
namespace clustering {
TupleSorter::TupleSorter(HeapTupleSorterOptions options) : options_(options) {
  Init();
}

TupleSorter::~TupleSorter() { DeInit(); }

void TupleSorter::Init() {
  CBDB_WRAP_START;
  {
    sort_state_ = tuplesort_begin_heap(
        options_.tup_desc, options_.nkeys, options_.attr,
        options_.sortOperators, options_.sortCollations,
        &options_.nulls_first_flags, options_.work_mem, NULL, false);
  }
  CBDB_WRAP_END;
}

void TupleSorter::DeInit() {
  CBDB_WRAP_START;
  { tuplesort_end(sort_state_); }
  CBDB_WRAP_END;
}

void TupleSorter::AppendSortData(TupleTableSlot *slot) {
  // FIXME(gongxun): performance issue with CBDB_WRAP??
  CBDB_WRAP_START;
  { tuplesort_puttupleslot(sort_state_, slot); }
  CBDB_WRAP_END;
}

void TupleSorter::Sort() {
  CBDB_WRAP_START;
  { tuplesort_performsort(sort_state_); }
  CBDB_WRAP_END;
}

bool TupleSorter::GetSortedData(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { return tuplesort_gettupleslot(sort_state_, true, false, slot, NULL); }
  CBDB_WRAP_END;
}

}  // namespace clustering

}  // namespace pax
