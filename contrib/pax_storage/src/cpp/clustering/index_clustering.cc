
#include "clustering/index_clustering.h"

#include "clustering/sorter_index.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
namespace clustering {

IndexClustering::IndexClustering() {}
IndexClustering::~IndexClustering() {}
void IndexClustering::Clustering(ClusteringDataReader *reader,
                                 ClusteringDataWriter *writer,
                                 const DataClusteringOptions *options) {
  const IndexClusteringOptions *index_options =
      reinterpret_cast<const IndexClusteringOptions *>(options);

  CBDB_CHECK(index_options != nullptr && index_options->index_rel != nullptr,
             cbdb::CException::kExTypeInvalid, "index options is invalid");

  TupleTableSlot *origin_slot;
  TupleTableSlot *sorted_slot;
  IndexSorter::IndexTupleSorterOptions sorter_options;

  sorter_options.tup_desc = index_options->tup_desc;
  sorter_options.work_mem = index_options->work_mem;
  sorter_options.index_rel = index_options->index_rel;

  IndexSorter sorter(sorter_options);

  origin_slot =
      cbdb::MakeSingleTupleTableSlot(index_options->tup_desc, &TTSOpsVirtual);
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