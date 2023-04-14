#pragma once

#include <string>

namespace pax {
class MicroPartitionMetadata {
 public:
  MicroPartitionMetadata(const std::string micro_partition_id,
                         const std::string filename)
      : micro_partition_id_(micro_partition_id), file_name_(filename) {}

  ~MicroPartitionMetadata() {}

  const std::string getMicroPartitionId() const { return micro_partition_id_; }

  const std::string getFileName() const { return file_name_; }

  void setTupleCount(uint32_t tuple_count) { tuple_count_ = tuple_count; }
  uint32_t const getTupleCount() const { return tuple_count_; }

  void setFileSize(uint32_t file_size) { file_size_ = file_size; }
  uint32_t const getFileSize() const { return file_size_; }

 private:
  const std::string micro_partition_id_;

  std::string file_name_;

  // statistics info
  uint32_t tuple_count_ = 0;
  uint32_t file_size_ = 0;

  // TODO(gongxun): add more info like bloom filter, index, etc.
};  // class MicroPartitionMetadata
}  // namespace pax
