#pragma once

#include "comm/cbdb_api.h"
namespace pax {

namespace clustering {

class ClusteringDataWriter {
 public:
  ClusteringDataWriter() = default;
  virtual ~ClusteringDataWriter() = default;
  /**
   * slot->tts_value[tuple_desc->natts] is zorder_value
   */
  virtual void WriteTuple(TupleTableSlot *tuple) = 0;
  virtual void Close() = 0;
};

}  // namespace clustering

}  // namespace pax
