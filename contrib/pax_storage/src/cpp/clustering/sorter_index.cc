
#include "clustering/sorter_index.h"

#include "comm/cbdb_wrappers.h"

namespace pax {
namespace clustering {
IndexSorter::IndexSorter(IndexTupleSorterOptions options) : options_(options) {
  Init();
}

IndexSorter::~IndexSorter() { DeInit(); }

void IndexSorter::Init() {
  CBDB_WRAP_START;
  {
    sort_state_ = tuplesort_begin_cluster(options_.tup_desc, options_.index_rel,
                                          options_.work_mem, NULL, false);
  }
  CBDB_WRAP_END;
}

void IndexSorter::DeInit() {
  CBDB_WRAP_START;
  { tuplesort_end(sort_state_); }
  CBDB_WRAP_END;
}

void IndexSorter::AppendSortData(TupleTableSlot *slot) {
  Datum *slot_values;
  bool *slot_isnull;
  HeapTuple tuple;

  CBDB_WRAP_START;
  {
    slot_getallattrs(slot);
    slot_values = slot->tts_values;
    slot_isnull = slot->tts_isnull;

    tuple =
        heap_form_tuple(slot->tts_tupleDescriptor, slot_values, slot_isnull);

    tuplesort_putheaptuple(sort_state_, tuple);
    heap_freetuple(tuple);
  }
  CBDB_WRAP_END;
}
void IndexSorter::Sort() {
  CBDB_WRAP_START;
  { tuplesort_performsort(sort_state_); }
  CBDB_WRAP_END;
}

bool IndexSorter::GetSortedData(TupleTableSlot *slot) {
  HeapTuple tuple;

  CBDB_WRAP_START;
  {
    tuple = tuplesort_getheaptuple(sort_state_, true);
    if (tuple == NULL) return false;

    ExecClearTuple(slot);

    heap_deform_tuple(tuple, options_.tup_desc, slot->tts_values,
                      slot->tts_isnull);

    ExecStoreVirtualTuple(slot);
  }
  CBDB_WRAP_END;
  return true;
}

}  // namespace clustering

}  // namespace pax
