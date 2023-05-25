#include "storage/strategy.h"

namespace pax {

size_t PaxDefaultSplitStrategy::SplitTupleNumbers() const { return 100000; }

size_t PaxDefaultSplitStrategy::SplitFileSize() const {
  return 64 * 1024 * 1024;
}

bool PaxDefaultSplitStrategy::ShouldSplit(MicroPartitionWriter *writer,
                                          size_t num_tuples) const {
  return (num_tuples >= SplitTupleNumbers()) ||
         (writer->EstimatedSize() >= SplitFileSize());
}

}  // namespace pax
