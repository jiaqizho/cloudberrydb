#pragma once

#include <cstddef>

#include "storage/micro_partition.h"

namespace pax {

class FileSplitStrategy {
 public:
  virtual ~FileSplitStrategy() = default;

  virtual bool ShouldSplit(MicroPartitionWriter *writer,
                           size_t num_tuples) const = 0;

  virtual size_t SplitTupleNumbers() const = 0;

  virtual size_t SplitFileSize() const = 0;
};

class PaxDefaultSplitStrategy : public FileSplitStrategy {
 public:
  PaxDefaultSplitStrategy() = default;
  ~PaxDefaultSplitStrategy() override = default;

  size_t SplitTupleNumbers() const override;

  size_t SplitFileSize() const override;

  bool ShouldSplit(MicroPartitionWriter *writer,
                   size_t num_tuples) const override;
};
}  // namespace pax
