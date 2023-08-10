#include "storage/micro_partition_metadata.h"

namespace pax {

WriteSummary::WriteSummary()
    : file_size(0), num_tuples(0), rel_oid(InvalidOid) {}

MicroPartitionMetadata::MicroPartitionMetadata(std::string micro_partition_id,
                                               std::string filename)
    : micro_partition_id_(micro_partition_id), file_name_(filename) {}

MicroPartitionMetadata::MicroPartitionMetadata(
    MicroPartitionMetadata &&other) noexcept {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  file_size_ = other.file_size_;
}

MicroPartitionMetadata &MicroPartitionMetadata::operator=(
    MicroPartitionMetadata &&other) noexcept {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  file_size_ = other.file_size_;
  return *this;
}

const std::string &MicroPartitionMetadata::GetMicroPartitionId() const {
  return micro_partition_id_;
}

const std::string &MicroPartitionMetadata::GetFileName() const {
  return file_name_;
}

void MicroPartitionMetadata::SetTupleCount(uint32 tuple_count) {
  tuple_count_ = tuple_count;
}

uint32 MicroPartitionMetadata::GetTupleCount() const { return tuple_count_; }

void MicroPartitionMetadata::SetFileSize(uint32 file_size) {
  file_size_ = file_size;
}

uint32 MicroPartitionMetadata::GetFileSize() const { return file_size_; }
}  // namespace pax
