#pragma once

#include "clustering/clustering.h"
#include "clustering/clustering_reader.h"
#include "clustering/clustering_writer.h"
namespace pax {
namespace clustering {
class ZOrderClustering final : public DataClustering {
 public:
  struct ZOrderClusteringOptions : public DataClusteringOptions {
    ZOrderClusteringOptions() { type = kClusterTypeZOrder; }
    TupleDesc tup_desc;
    AttrNumber *attr;
    int nkeys;
    bool nulls_first_flags;
    int work_mem;
  };
  ZOrderClustering();
  virtual ~ZOrderClustering();
  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;

 private:
  void CheckOptions(const ZOrderClusteringOptions *options);
  // TODO(gongxun): support batch size
  void MakeZOrderTupleSlot(TupleTableSlot *zorder_slot,
                           const TupleTableSlot *slot,
                           const ZOrderClusteringOptions *options,
                           char *column_bytes_buffer, int buffer_len);
};

}  // namespace clustering

}  // namespace pax
