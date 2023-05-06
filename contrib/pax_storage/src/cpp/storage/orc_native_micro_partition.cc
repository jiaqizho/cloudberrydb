#include "storage/orc_native_micro_partition.h"

#include "comm/cbdb_api.h"

#include <utility>

#include "catalog/pax_aux_table.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "storage/file_system.h"

namespace pax {

OrcNativeMicroPartitionWriter::OrcNativeMicroPartitionWriter(
    const WriterOptions& options, FileSystemPtr fs)
    : MicroPartitionWriter(options, fs) {}

void OrcNativeMicroPartitionWriter::Create() {
  File* file;
  file = file_system_->Open(writer_options_.file_name);
  Assert(file != nullptr);

  OrcFileWriter::OrcWriterOptions orc_options;
  orc_options.desc = writer_options_.desc;
  orc_options.buffer_size = writer_options_.buffer_size;
  orc_options.batch_row_size = 1024;
  orc_options.stripe_size = 1024 * 1024;

  writer_ = new OrcFileWriter(file, orc_options);

  summary_.block_id = writer_options_.block_id;
  summary_.file_name = writer_options_.file_name;
  summary_.file_size = 0;
  summary_.num_tuples = 0;
}

void OrcNativeMicroPartitionWriter::Close() {
  if (writer_) {
    writer_->Close();
    summary_.file_size = writer_->WriteSize();
    AddMicroPartitionEntry();
    delete writer_;
    writer_ = nullptr;
  }
}

size_t OrcNativeMicroPartitionWriter::EstimatedSize() const {
  return writer_->WriteSize();
}

void OrcNativeMicroPartitionWriter::WriteTuple(CTupleSlot* slot) {
  // todo: slot to column format
  writer_->ParseTupleAndWrite(slot->GetTupleTableSlot());
  summary_.num_tuples++;
  // todo summary_callback
}

void OrcNativeMicroPartitionWriter::WriteTupleN(CTupleSlot** slot, size_t n) {
  // todo need covert tupleN in batch rowset, and use batch write
  for (size_t i = 0; i < n; i++) {
    WriteTuple(slot[i]);
  }
}

const std::string OrcNativeMicroPartitionWriter::FullFileName() const {
  return this->writer_options_.file_name;
}

void OrcNativeMicroPartitionWriter::AddMicroPartitionEntry() {
  if (this->summary_callback_) {
    summary_callback_(summary_);
  }
}

OrcNativeMicroPartitionReader::OrcNativeMicroPartitionReader(
    const FileSystemPtr& fs)
    : MicroPartitionReader(fs) {}

OrcNativeMicroPartitionReader::~OrcNativeMicroPartitionReader() {}

void OrcNativeMicroPartitionReader::Open(const ReaderOptions& options) {
  File* file = file_system_->Open(options.file_name);
  Assert(file != nullptr);

  reader_ = new OrcFileReader(file);
}
void OrcNativeMicroPartitionReader::Close() {
  if (reader_) {
    reader_->Close();
    delete reader_;
    reader_ = nullptr;
  }
}
bool OrcNativeMicroPartitionReader::ReadTuple(CTupleSlot* slot) {
  Assert(reader_);
  return reader_->ReadNextBatch(slot);
}
size_t OrcNativeMicroPartitionReader::Length() const {
  // TODO(gongxun): get length from orc file
  Assert(false);
  return 0;
}

void OrcNativeMicroPartitionReader::Seek(size_t offset) {
  reader_->SeekToRow(offset);
}

uint64_t OrcNativeMicroPartitionReader::Offset() const {
  return reader_->Offset();
}

size_t OrcNativeMicroPartitionReader::NumTuples() const {
  // TODO(gongxun): get num tuples from orc file
  Assert(false);
  return 0;
}

}  // namespace pax
