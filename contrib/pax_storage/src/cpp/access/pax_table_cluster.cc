#include "access/pax_table_cluster.h"

#include "clustering/clustering.h"
#include "clustering/index_clustering.h"
#include "clustering/pax_clustering_reader.h"
#include "clustering/pax_clustering_writer.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"

#define CLUSTER_SORT_MEMORY 128000

namespace pax {
void IndexCluster(Relation old_rel, Relation new_rel, Relation index,
                  Snapshot snapshot) {
  auto iter = MicroPartitionInfoIterator::New(old_rel, snapshot);

  auto reader =
      PAX_NEW<clustering::PaxClusteringReader>(old_rel, std::move(iter));

  auto writer = PAX_NEW<clustering::PaxClusteringWriter>(new_rel);

  auto cluster = clustering::DataClustering::CreateDataClustering(
      clustering::DataClustering::kClusterTypeIndex);

  clustering::IndexClustering::IndexClusteringOptions options;

  options.tup_desc = old_rel->rd_att;
  options.index_rel = index;
  options.work_mem = CLUSTER_SORT_MEMORY;
  cluster->Clustering(reader, writer, &options);

  writer->Close();
  PAX_DELETE(writer);
  reader->Close();
  PAX_DELETE(reader);
}

}  // namespace pax