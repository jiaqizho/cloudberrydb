#pragma once

#include <memory>

#include "clustering/clustering_reader.h"
#include "comm/iterator.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
namespace clustering {
class PaxClusteringReader final : public ClusteringDataReader {
 public:
  PaxClusteringReader(
      Relation rel,
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);
  virtual ~PaxClusteringReader();
  bool GetNextTuple(TupleTableSlot *) override;
  void Close() override;

 private:
  Relation relation_ = nullptr;
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iter_;
  MicroPartitionReader *reader_ = nullptr;
  FileSystem *file_system_ = nullptr;
  FileSystemOptions *file_system_options_ = nullptr;
};
}  // namespace clustering
}  // namespace pax
