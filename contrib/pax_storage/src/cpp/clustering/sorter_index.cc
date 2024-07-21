
#include "clustering/sorter_index.h"

namespace pax {
namespace clustering {
IndexSorter::IndexSorter(IndexTupleSorterOptions options) : options_(options) {
  Init();
}

IndexSorter::~IndexSorter() { DeInit(); }

void IndexSorter::Init() {
  sort_state_ = tuplesort_begin_cluster(options_.tup_desc, options_.index_rel,
                                        options_.work_mem, NULL, false);
}

void IndexSorter::DeInit() { tuplesort_end(sort_state_); }

void IndexSorter::AppendSortData(TupleTableSlot *slot) {
  Datum *slot_values;
  bool *slot_isnull;
  HeapTuple tuple;
  CHECK_FOR_INTERRUPTS();

  slot_getallattrs(slot);
  slot_values = slot->tts_values;
  slot_isnull = slot->tts_isnull;

  tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot_values, slot_isnull);

  tuplesort_putheaptuple(sort_state_, tuple);
  heap_freetuple(tuple);
}
void IndexSorter::Sort() { tuplesort_performsort(sort_state_); }

bool IndexSorter::GetSortedData(TupleTableSlot *slot) {
  HeapTuple tuple;

  tuple = tuplesort_getheaptuple(sort_state_, true);
  if (tuple == NULL) return false;

  ExecClearTuple(slot);

  heap_deform_tuple(tuple, options_.tup_desc, slot->tts_values,
                    slot->tts_isnull);

  ExecStoreVirtualTuple(slot);
  return true;
}

}  // namespace clustering

}  // namespace pax
