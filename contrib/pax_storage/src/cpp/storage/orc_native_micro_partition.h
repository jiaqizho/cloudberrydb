#pragma once

#include "micro_partition.h"
#include "orc_file_stream.h"
#include <string>

namespace pax {
class OrcNativeMicroPartitionWriter : public MicroPartitionWriter {
 public:
  OrcNativeMicroPartitionWriter(const WriterOptions& options, FileSystemPtr fs);
  
  // if options.file_name is empty, generate new file name
  void Create() override;

  // close the current write file. Create may be called after Close
  // to write a new micro partition.
  virtual void Close() override;

  // estimated size of the writing size, used to determine
// whether to switch to another micro partition.
  virtual size_t EstimatedSize() const override;

  // append tuple to the current micro partition file
  // return the number of tuples the current micro partition has written
  virtual void WriteTuple(CTupleSlot *slot) override;
  virtual void WriteTupleN(CTupleSlot **slot, size_t n) override;

  // returns the full file name(including directory path) used for write.
  // normally it's <tablespace_dir>/<database_id>/<file_name> for local files,
  // depends on the write options
  virtual const std::string FullFileName() const override;

private:
  void AddMicroPartitionEntry();
  std::string block_id_;
  WriteSummary summary_;
  OrcFileWriter* writer_;
};
}  // namespace pax
