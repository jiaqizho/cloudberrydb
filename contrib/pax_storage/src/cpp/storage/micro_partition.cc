
#include "storage/micro_partition.h"
#include <utility>

namespace pax {

MicroPartitionWriter* MicroPartitionWriter::SetWriteSummaryCallback
    (std::function<void(const WriteSummary &summary)> &&callback) {
    summary_callback_ = std::move(callback);
    return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    StatsCollector *collector) {
    Assert(collector_ == nullptr);
    collector_ = collector;
    return this;
}

const MicroPartitionWriter::WriterOptions & MicroPartitionWriter::Options() const {
    return writer_options_;
}

const std::string & MicroPartitionWriter::FileName() const {
    return writer_options_.file_name;
}


const MicroPartitionReader::ReaderOptions &MicroPartitionReader::options() const {
    return options_;
}

const std::string &MicroPartitionReader::file_name() const {
    return options_.file_name;
}

}  // namespace pax
