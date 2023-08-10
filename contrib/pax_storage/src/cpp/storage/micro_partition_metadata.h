#pragma once
#include "comm/cbdb_api.h"

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
  WriteSummary();
  WriteSummary(const WriteSummary &summary) = default;
};

class MicroPartitionMetadata {
 public:
  MicroPartitionMetadata(std::string micro_partition_id, std::string filename);

  ~MicroPartitionMetadata() = default;

  MicroPartitionMetadata(const MicroPartitionMetadata &other) = default;

  MicroPartitionMetadata(MicroPartitionMetadata &&other) noexcept;

  MicroPartitionMetadata &operator=(const MicroPartitionMetadata &other) =
      default;

  MicroPartitionMetadata &operator=(MicroPartitionMetadata &&other) noexcept;

  const std::string &GetMicroPartitionId() const;

  const std::string &GetFileName() const;

  void SetTupleCount(uint32 tuple_count);

  uint32 GetTupleCount() const;

  void SetFileSize(uint32 file_size);

  uint32 GetFileSize() const;

 private:
  std::string micro_partition_id_;

  std::string file_name_;

  // statistics info
  uint32 tuple_count_ = 0;
  uint32 file_size_ = 0;

  // TODO(gongxun): add more info like bloom filter, index, etc.
};  // class MicroPartitionMetadata
}  // namespace pax
