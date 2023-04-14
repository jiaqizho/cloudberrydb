#include "catalog/table_metadata.h"
namespace pax {
std::shared_ptr<TableMetadata::Iterator> TableMetadata::NewIterator() {
  auto micro_partitions =
      std::make_shared<std::vector<std::shared_ptr<MicroPartitionMetadata>>>();

  getAllMicroPartitionMetadata(micro_partitions);

  return std::shared_ptr<Iterator>(Iterator::Create(micro_partitions));
}
}  // namespace pax
