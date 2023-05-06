#pragma once

#include <string>

#include "storage/micro_partition.h"
#include "storage/orc_file_stream.h"

namespace pax {
class OrcNativeMicroPartitionWriter : public MicroPartitionWriter {
 public:
  OrcNativeMicroPartitionWriter(const WriterOptions& options, FileSystemPtr fs);

  // if options.file_name is empty, generate new file name
  void Create() override;

  // close the current write file. Create may be called after Close
  // to write a new micro partition.
  void Close() override;

  // estimated size of the writing size, used to determine
  // whether to switch to another micro partition.
  size_t EstimatedSize() const override;

  // append tuple to the current micro partition file
  // return the number of tuples the current micro partition has written
  void WriteTuple(CTupleSlot* slot) override;
  void WriteTupleN(CTupleSlot** slot, size_t n) override;

  // returns the full file name(including directory path) used for write.
  // normally it's <tablespace_dir>/<database_id>/<file_name> for local files,
  // depends on the write options
  const std::string FullFileName() const override;

 private:
  void AddMicroPartitionEntry();
  std::string block_id_;
  WriteSummary summary_;
  OrcFileWriter* writer_;
};

class OrcNativeMicroPartitionReader : public MicroPartitionReader {
 public:
  explicit OrcNativeMicroPartitionReader(const FileSystemPtr& fs);

  virtual ~OrcNativeMicroPartitionReader();

  void Open(const ReaderOptions& options) override;

  void Close() override;

  bool ReadTuple(CTupleSlot* slot) override;

  void Seek(size_t offset) override;

  uint64_t Offset() const override;

  size_t Length() const override;

  size_t NumTuples() const override;

  void SetFilter(Filter* filter) override {}

 private:
  std::string block_id_;
  OrcFileReader* reader_;
};

}  // namespace pax
