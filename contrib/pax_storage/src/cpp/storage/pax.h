#pragma once
#include <optional>

#include "storage/micro_partition.h"

namespace pax {

class TableWriter {
 public:
    class FileSplitStrategy {
     public:
        virtual bool ShouldSplit(MicroPartitionWriter *writer, size_t num_tuples) = 0;
    };
    using FileSplitStrategyPtr = FileSplitStrategy *;

 public:
    TableWriter(const MicroPartitionWriterPtr writer, const FileSplitStrategyPtr strategy) :
        writer_(writer),
        strategy_(strategy) {}

    explicit TableWriter(const MicroPartitionWriterPtr writer) : TableWriter(writer, nullptr) {}

    virtual void WriteTuple(CTupleSlot *slot);

    virtual void Open() {}

    virtual void Close() {}

    size_t total_tuples() const;

 protected:
    MicroPartitionWriterPtr writer_;
    FileSplitStrategy * strategy_;
    size_t num_tuples_ = 0;
    size_t total_tuples_ = 0;
};

template <typename elements = MicroPartitionReader::ReaderOptions,
typename iterator_tag = std::bidirectional_iterator_tag>
class TableReader {
 public:
    explicit TableReader(std::iterator<iterator_tag, elements> iterator):
        iterator_(iterator) {}

    virtual void Open() {}

    virtual void Close() {}

    size_t num_tuples() const {
        return num_tuples_;
    }

    virtual CTupleSlot *ReadTuple(CTupleSlot *slot) = 0;
 protected:
     MicroPartitionReader *reader_ = nullptr;
     std::iterator<iterator_tag, elements> iterator_;
     size_t num_tuples_ = 0;
};

}  // namespace pax
