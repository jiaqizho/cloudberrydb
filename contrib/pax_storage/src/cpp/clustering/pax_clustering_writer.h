#pragma once

#include "comm/cbdb_api.h"

#include "clustering/clustering_writer.h"
#include "storage/pax.h"

namespace pax {
namespace clustering {
class PaxClusteringWriter final : public ClusteringDataWriter {
 public:
  PaxClusteringWriter(Relation rel);
  virtual ~PaxClusteringWriter();
  // If in zorder_cluster, the last column of the slot in WriteTuple is
  // zorder_value
  void WriteTuple(TupleTableSlot *tuple) override;
  void Close() override;

 private:
  Relation rel_ = nullptr;
  std::unique_ptr<TableWriter> writer_ = nullptr;
};
}  // namespace clustering

}  // namespace pax
