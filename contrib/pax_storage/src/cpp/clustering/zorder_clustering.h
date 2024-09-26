#pragma once

#include "clustering/clustering.h"
#include "clustering/clustering_reader.h"
#include "clustering/clustering_writer.h"
#include "comm/pax_memory.h"
namespace pax {
namespace clustering {
class ZOrderClustering final : public DataClustering {
 public:
  struct ZOrderClusteringOptions : public DataClusteringOptions {
    ZOrderClusteringOptions(TupleDesc tup_desc, int nkeys,
                            bool nulls_first_flags, int work_mem) {
      type = kClusterTypeZOrder;

      this->tup_desc = tup_desc;
      this->nkeys = nkeys;
      this->nulls_first_flags = nulls_first_flags;
      this->work_mem = work_mem;
      attr = PAX_NEW_ARRAY<AttrNumber>(nkeys);
    }
    ~ZOrderClusteringOptions() { PAX_DELETE_ARRAY(attr); }
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
