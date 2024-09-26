#include "clustering/clustering.h"

#include "clustering/index_clustering.h"
#include "clustering/lexical_clustering.h"
#include "clustering/zorder_clustering.h"
#include "comm/pax_memory.h"

namespace pax {
namespace clustering {

DataClustering *DataClustering::CreateDataClustering(
    const DataClustering::ClusterType type) {
  switch (type) {
    case DataClustering::kClusterTypeZOrder:
      static ZOrderClustering zorder_clustering;
      return &zorder_clustering;
    case DataClustering::kClusterTypeIndex:
      static IndexClustering index_clustering;
      return &index_clustering;
    case DataClustering::kClusterTypeLexical:
      static LexicalClustering lexical_clustering;
      return &lexical_clustering;
    default:
      return nullptr;
  }
}

}  // namespace clustering
}  // namespace pax
