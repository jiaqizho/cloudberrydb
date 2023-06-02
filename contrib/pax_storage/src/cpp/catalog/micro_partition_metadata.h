#pragma once

#include <string>
#include <utility>

namespace pax {
// WriteSummary is generated after the current micro partition is flushed and
// closed.
struct WriteSummary {
  std::string file_name;
  std::string block_id;
  size_t file_size;
  size_t num_tuples;
  unsigned int rel_oid;
  WriteSummary() : file_size(0), num_tuples(0) {}
  WriteSummary(const WriteSummary& summary)
      : file_name(summary.file_name),
        block_id(summary.block_id),
        file_size(summary.file_size),
        num_tuples(summary.num_tuples),
        rel_oid(summary.rel_oid) {}
};
class MicroPartitionMetadata {
 public:
  MicroPartitionMetadata(const std::string micro_partition_id,
                         const std::string filename)
      : micro_partition_id_(micro_partition_id), file_name_(filename) {}

  ~MicroPartitionMetadata() {}

  MicroPartitionMetadata(const MicroPartitionMetadata& other)
      : micro_partition_id_(other.micro_partition_id_),
        file_name_(other.file_name_),
        tuple_count_(other.tuple_count_),
        file_size_(other.file_size_) {}

  MicroPartitionMetadata(MicroPartitionMetadata&& other) {
    micro_partition_id_ = std::move(other.micro_partition_id_);
    file_name_ = std::move(other.file_name_);
    tuple_count_ = other.tuple_count_;
    file_size_ = other.file_size_;
  }

  MicroPartitionMetadata& operator=(const MicroPartitionMetadata& other) {
    micro_partition_id_ = other.micro_partition_id_;
    file_name_ = other.file_name_;
    tuple_count_ = other.tuple_count_;
    file_size_ = other.file_size_;
    return *this;
  }

  MicroPartitionMetadata& operator=(MicroPartitionMetadata&& other) {
    micro_partition_id_ = std::move(other.micro_partition_id_);
    file_name_ = std::move(other.file_name_);
    tuple_count_ = other.tuple_count_;
    file_size_ = other.file_size_;
    return *this;
  }

  const std::string& getMicroPartitionId() const { return micro_partition_id_; }

  const std::string& getFileName() const { return file_name_; }

  void setTupleCount(uint32_t tuple_count) { tuple_count_ = tuple_count; }
  uint32_t const getTupleCount() const { return tuple_count_; }

  void setFileSize(uint32_t file_size) { file_size_ = file_size; }
  uint32_t const getFileSize() const { return file_size_; }

 private:
  std::string micro_partition_id_;

  std::string file_name_;

  // statistics info
  uint32_t tuple_count_ = 0;
  uint32_t file_size_ = 0;

  // TODO(gongxun): add more info like bloom filter, index, etc.
};  // class MicroPartitionMetadata
}  // namespace pax
