
#pragma once

#include "clustering/clustering.h"
#include "comm/pax_memory.h"

namespace pax {
namespace clustering {
class LexicalClustering final : public DataClustering {
 public:
  struct LexicalClusteringOptions : public DataClusteringOptions {
    LexicalClusteringOptions(TupleDesc tup_desc, int nkeys,
                             bool nulls_first_flags, int work_mem) {
      type = kClusterTypeLexical;

      this->tup_desc = tup_desc;
      this->nkeys = nkeys;
      this->nulls_first_flags = nulls_first_flags;
      this->work_mem = work_mem;
      attr = PAX_NEW_ARRAY<AttrNumber>(nkeys);
      sortOperators = PAX_NEW_ARRAY<Oid>(nkeys);
      sortCollations = PAX_NEW_ARRAY<Oid>(nkeys);
    }

    ~LexicalClusteringOptions() {
      PAX_DELETE_ARRAY(attr);
      PAX_DELETE_ARRAY(sortOperators);
      PAX_DELETE_ARRAY(sortCollations);
    }
    TupleDesc tup_desc;
    AttrNumber *attr;
    Oid *sortOperators;
    Oid *sortCollations;
    int nkeys;
    bool nulls_first_flags;
    int work_mem;
  };

  LexicalClustering();
  virtual ~LexicalClustering();

  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;
};
}  // namespace clustering

}  // namespace pax