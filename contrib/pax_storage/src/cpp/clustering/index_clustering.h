#pragma once

#include "clustering/clustering.h"
namespace pax {
namespace clustering {
class IndexClustering final : public DataClustering {
 public:
  struct IndexClusteringOptions : public DataClusteringOptions {
    IndexClusteringOptions() { type = kClusterTypeIndex; }
    TupleDesc tup_desc;
    Relation index_rel;
    int work_mem;
  };

 public:
  IndexClustering();
  virtual ~IndexClustering();
  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;
};

}  // namespace clustering

}  // namespace pax
