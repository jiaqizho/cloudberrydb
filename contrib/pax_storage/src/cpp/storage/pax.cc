

#include "storage/pax.h"

namespace pax {


void TableWriter::WriteTuple(CTupleSlot *slot) {
    Assert(writer_);
    writer_->WriteTuple(slot);
    ++num_tuples_;
    ++total_tuples_;

    if (strategy_ && strategy_->ShouldSplit(writer_, num_tuples_)) {
        writer_->Close();
        writer_->Create();
        num_tuples_ = 0;
    }
}

size_t TableWriter::total_tuples() const {
    return total_tuples_;
}

}  // namespace pax
