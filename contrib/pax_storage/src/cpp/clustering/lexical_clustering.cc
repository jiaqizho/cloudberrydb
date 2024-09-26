#include "clustering/lexical_clustering.h"

#include "clustering/sorter_tuple.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
namespace clustering {

LexicalClustering::LexicalClustering() {}

LexicalClustering::~LexicalClustering() {}

void LexicalClustering::Clustering(ClusteringDataReader *reader,
                                  ClusteringDataWriter *writer,
                                  const DataClusteringOptions *options) {
  const LexicalClusteringOptions *lexical_options =
      reinterpret_cast<const LexicalClusteringOptions *>(options);

  TupleTableSlot *origin_slot;
  TupleTableSlot *sorted_slot;
  TupleSorter::HeapTupleSorterOptions sorter_options;

  sorter_options.tup_desc = lexical_options->tup_desc;
  sorter_options.work_mem = lexical_options->work_mem;
  sorter_options.nulls_first_flags = lexical_options->nulls_first_flags;
  sorter_options.nkeys = lexical_options->nkeys;
  sorter_options.attr = lexical_options->attr;
  sorter_options.sortCollations = lexical_options->sortCollations;
  sorter_options.sortOperators = lexical_options->sortOperators;

  TupleSorter sorter(sorter_options);

  origin_slot =
      cbdb::MakeSingleTupleTableSlot(lexical_options->tup_desc, &TTSOpsVirtual);
  while (reader->GetNextTuple(origin_slot)) {
    sorter.AppendSortData(origin_slot);
  }

  cbdb::ExecDropSingleTupleTableSlot(origin_slot);

  sorter.Sort();

  sorted_slot = cbdb::MakeSingleTupleTableSlot(sorter_options.tup_desc,
                                               &TTSOpsMinimalTuple);
  while (sorter.GetSortedData(sorted_slot)) {
    cbdb::SlotGetAllAttrs(sorted_slot);
    writer->WriteTuple(sorted_slot);
  }
  cbdb::ExecDropSingleTupleTableSlot(sorted_slot);
}
}  // namespace clustering

}  // namespace pax
