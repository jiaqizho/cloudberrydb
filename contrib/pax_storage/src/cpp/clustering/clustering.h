#pragma once

#include "clustering/clustering_reader.h"
#include "clustering/clustering_writer.h"
#include "clustering/sorter.h"

namespace pax {

namespace clustering {

class DataClustering {
 public:
  enum ClusterType {
    kClusterTypeIndex,
  };

  struct DataClusteringOptions {
    ClusterType type;
  };

 public:
  DataClustering() = default;
  virtual ~DataClustering() = default;
  virtual void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                          const DataClusteringOptions *options) = 0;
  static DataClustering *CreateDataClustering(const ClusterType type);
};

}  // namespace clustering
}  // namespace pax
