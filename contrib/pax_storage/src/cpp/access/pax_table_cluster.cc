#include "access/pax_table_cluster.h"

#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "clustering/clustering.h"
#include "clustering/index_clustering.h"
#include "clustering/pax_clustering_reader.h"
#include "clustering/pax_clustering_writer.h"
#include "clustering/zorder_clustering.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"

#define CLUSTER_SORT_MEMORY 128000

namespace pax {
void DeleteClusteringFiles(
    Relation rel, Snapshot snapshot,
    std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter) {
  while (iter->HasNext()) {
    auto it = iter->Next();
    cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel), snapshot,
                                    it.GetMicroPartitionId());
  }
  // TODO(gongxun): mark files deleted or delete files directly?
}

void ZOrderCluster(Relation rel, Snapshot snapshot,
                   bool is_incremental_cluster) {
  auto columns = cbdb::GetClusterColumnsIndex(rel);

  CBDB_CHECK(!columns.empty(), cbdb::CException::kExTypeInvalid);

  std::vector<MicroPartitionMetadata> delete_files;
  auto iter = MicroPartitionInfoIterator::New(rel, snapshot);
  auto wrap = PAX_NEW<FilterIterator<MicroPartitionMetadata>>(
      std::move(iter),
      [&delete_files, is_incremental_cluster](const MicroPartitionMetadata &x) {
        // if is incremental cluster, only cluster the non-clustered blocks
        if (is_incremental_cluster && x.IsClustered()) {
          return false;
        }
        delete_files.push_back(x);
        return true;
      });
  iter = std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(wrap);

  auto reader = PAX_NEW<clustering::PaxClusteringReader>(rel, std::move(iter));

  auto writer = PAX_NEW<clustering::PaxClusteringWriter>(rel);

  // create cluster, singleton
  auto cluster = clustering::DataClustering::CreateDataClustering(
      clustering::DataClustering::kClusterTypeZOrder);

  clustering::ZOrderClustering::ZOrderClusteringOptions options;
  options.nkeys = columns.size();
  
  AttrNumber attrs[options.nkeys];
  for (int i = 0; i < options.nkeys; i++) {
    // AttrNumer is columns_index + 1
    attrs[i] = columns[i] + 1;
  }
  options.attr = attrs;
  options.tup_desc = rel->rd_att;
  options.nulls_first_flags = false;
  options.work_mem = maintenance_work_mem;

  cluster->Clustering(reader, writer, &options);

  writer->Close();
  PAX_DELETE(writer);
  reader->Close();
  PAX_DELETE(reader);

  std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter_ptr =
      std::make_shared<VectorIterator<MicroPartitionMetadata>>(
          std::move(delete_files));
  DeleteClusteringFiles(rel, snapshot, iter_ptr);
}

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
  options.work_mem = maintenance_work_mem;
  cluster->Clustering(reader, writer, &options);

  writer->Close();
  PAX_DELETE(writer);
  reader->Close();
  PAX_DELETE(reader);
}

}  // namespace pax