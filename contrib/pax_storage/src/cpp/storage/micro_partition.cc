#include "storage/micro_partition.h"

#include <utility>

#include "storage/pax_itemptr.h"
#include "storage/pax_filter.h"

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

MicroPartitionReaderProxy::~MicroPartitionReaderProxy() {
  delete reader_;
}

void MicroPartitionReaderProxy::Open(const MicroPartitionReader::ReaderOptions &options) {
  Assert(reader_);
  reader_->Open(options);
}

void MicroPartitionReaderProxy::Close() {
  Assert(reader_);
  reader_->Close();
}

bool MicroPartitionReaderProxy::ReadTuple(CTupleSlot *slot) {
  Assert(reader_);
  return reader_->ReadTuple(slot);
}

void MicroPartitionReaderProxy::SetReader(MicroPartitionReader *reader) {
  Assert(reader);
  Assert(!reader_);
  reader_ = reader;
}

size_t MicroPartitionReaderProxy::GetGroupNums() { return reader_->GetGroupNums(); }

std::unique_ptr<ColumnStatsProvider> MicroPartitionReaderProxy::GetGroupStatsInfo(size_t group_index) {
  return std::move(reader_->GetGroupStatsInfo(group_index));
}

MicroPartitionReader::Group *MicroPartitionReaderProxy::ReadGroup(size_t index) {
  return reader_->ReadGroup(index);
}

}  // namespace pax
