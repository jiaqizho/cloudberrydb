#pragma once

#include "storage/file_system.h"
#include "storage/statistics.h"
#include "comm/pax_def.h"

#ifndef PAX_INDEPENDENT_MODE

extern "C" {
    #include "executor/tuptable.h"
};
#else

using TupleTableSlot = void *;
#endif
#include <stddef.h>
#include <string>
#include <stdexcept>
#include <functional>
#include <vector>


namespace pax {

class CTupleSlot {
 public:
 private:
    TupleTableSlot *slot_;
};


// WriteSummary is generated after the current micro partition is flushed and closed.
struct WriteSummary {
    std::string file_name;
    size_t file_size;
    size_t num_tuples;
    WriteSummary(const WriteSummary &summary)
        : file_name(summary.file_name)
        , file_size(summary.file_size)
        , num_tuples(summary.num_tuples) {
    }
};

class MicroPartitionWriter;
using MicroPartitionWriterPtr = pax::MicroPartitionWriter *;

class MicroPartitionWriter {
 public:
    struct WriterOptions {
        std::string file_name;
        uint32_t rel_file_node;
        WriterOptions() = default;
        WriterOptions(const WriterOptions &options)
            : file_name(options.file_name) {
        }
    };

    explicit MicroPartitionWriter(const WriterOptions & writer_options) : writer_options_(writer_options) {}

    virtual ~MicroPartitionWriter() = default;

    // if options.file_name is empty, generate new file name
    virtual void Create() = 0;

    // close the current write file. Create may be called after Close
    // to write a new micro partition.
    virtual void Close() = 0;

    // estimated size of the writing size, used to determine
    // whether to switch to another micro partition.
    virtual size_t EstimatedSize() const = 0;

    // append tuple to the current micro partition file
    // return the number of tuples the current micro partition has written
    virtual void WriteTuple(CTupleSlot *slot) = 0;
    virtual void WriteTupleN(CTupleSlot **slot, size_t n) = 0;

    // returns the full file name(including directory path) used for write.
    // normally it's <tablespace_dir>/<database_id>/<file_name> for local files,
    // depends on the write options
    virtual std::string FullFileName() const = 0;

    // summary callback is invoked after the file is closed.
    // returns MicroPartitionWriter to enable chain call.
    virtual MicroPartitionWriter *SetWriteSummaryCallback(std::function<void(const WriteSummary &summary)> &&callback);

    virtual MicroPartitionWriter *SetStatsCollector(StatsCollector *collector);

    const WriterOptions & Options() const;

    // return the file name of the current micro partition, excluding its directory path
    const std::string &FileName() const;

 protected:
    std::function<void(const WriteSummary &summary)> summary_callback_;
    WriterOptions writer_options_;
    FileSystem *file_system_ = nullptr;
    StatsCollector *collector_ = nullptr;
};


class MicroPartitionReader {
 public:
    struct ReaderOptions {
        // file name(excluding directory path) for read
        std::string file_name;
        // additioinal info to initialize a reader.
    };

    virtual ~MicroPartitionReader() = default;

    virtual void Open(const ReaderOptions &options) = 0;

    // Close the current reader. It may be re-Open.
    virtual void Close() = 0;

    // read tuple from the micro partition with a filter.
    // the default behavior doesn't push the predicate down to
    // the low-level storage code.
    // returns the offset of the tuple in the micro partition
    // NOTE: the ctid is stored in slot, mapping from block_id to micro partition
    // is also created during this stage, no matter the map relation is needed or not.
    // We may optimize to avoid creating the map relation later.
    virtual CTupleSlot *ReadTuple(CTupleSlot *slot) = 0;

    virtual size_t Length() const = 0;

    // total number of tuples in this micro partition
    virtual size_t NumTuples() const = 0;

    const ReaderOptions &options() const;

    virtual const std::string &file_name() const;

    using Filter = Vector<uint16_t>;
    virtual void SetFilter(Filter *filter) = 0;

 protected:
    ReaderOptions *options_;
};

}  // namespace pax
