#include "storage/micro_partition.h"

#include <utility>

#include "storage/pax_itemptr.h"

namespace pax {

CTupleSlot::CTupleSlot(TupleTableSlot *tuple_slot)
    : slot_(tuple_slot), table_no_(0), block_number_(0), offset_(0) {}

void CTupleSlot::StoreVirtualTuple() {
  // TODO(gongxun): set tts_tid, how to get block number from block id
  slot_->tts_tid =
      PaxItemPointer::GetTupleId(table_no_, block_number_, offset_);
  slot_->tts_flags &= ~TTS_FLAG_EMPTY;
  slot_->tts_nvalid = slot_->tts_tupleDescriptor->natts;
}

TupleDesc CTupleSlot::GetTupleDesc() const {
  return slot_->tts_tupleDescriptor;
}

TupleTableSlot *CTupleSlot::GetTupleTableSlot() const { return slot_; }

MicroPartitionWriter::MicroPartitionWriter(const WriterOptions &writer_options)
    : writer_options_(writer_options) {}

MicroPartitionWriter *MicroPartitionWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  summary_callback_ = callback;
  return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  Assert(mpstats_ == nullptr);
  mpstats_ = mpstats;
  return this;
}

}  // namespace pax
