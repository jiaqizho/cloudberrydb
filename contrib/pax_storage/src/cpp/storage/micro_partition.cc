#include "storage/micro_partition.h"

#include <utility>

namespace pax {

MicroPartitionWriter *MicroPartitionWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  summary_callback_ = callback;
  return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    StatsCollector *collector) {
  Assert(collector_ == nullptr);
  collector_ = collector;
  return this;
}

const MicroPartitionWriter::WriterOptions &MicroPartitionWriter::Options()
    const {
  return writer_options_;
}

const std::string &MicroPartitionWriter::FileName() const {
  return writer_options_.file_name;
}

}  // namespace pax
