#pragma once

#include "comm/cbdb_api.h"

#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"
namespace pax {
void DeleteClusteringFiles(
    Relation rel, Snapshot snapshot,
    std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter);

void Cluster(Relation rel, Snapshot snapshot, bool is_incremental_cluster);

void IndexCluster(Relation old_rel, Relation new_rel, Relation index,
                  Snapshot snapshot);
}  // namespace pax
