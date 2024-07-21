#include "clustering/clustering.h"

#include "clustering/index_clustering.h"
#include "comm/pax_memory.h"

namespace pax {
namespace clustering {

DataClustering *DataClustering::CreateDataClustering(
    const DataClustering::ClusterType type) {
  switch (type) {
    case DataClustering::kClusterTypeIndex:
      static IndexClustering index_clustering;
      return &index_clustering;
    default:
      return nullptr;
  }
}

}  // namespace clustering
}  // namespace pax
