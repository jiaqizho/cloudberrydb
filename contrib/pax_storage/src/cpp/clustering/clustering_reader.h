#pragma once

#include "comm/cbdb_api.h"
namespace pax {

namespace clustering {
class ClusteringDataReader {
 public:
  ClusteringDataReader() = default;
  virtual ~ClusteringDataReader() = default;
  // TODO(gongxun): support record batch
  // return false if no more tuples
  virtual bool GetNextTuple(TupleTableSlot *) = 0;
  virtual void Close() = 0;
};

}  // namespace clustering

}  // namespace pax
