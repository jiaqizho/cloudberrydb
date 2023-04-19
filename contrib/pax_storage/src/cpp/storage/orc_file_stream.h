#pragma once
#include <memory>
#include <string>
#include <utility>

// TODO(gongxun): use our c++ custom assert
#include "assert.h"  // NOLINT
#include "orc/OrcFile.hh"
#include "storage/file_system.h"
#include "storage/micro_partition.h"

extern "C" {
#include "access/tupdesc.h"
#include "executor/tuptable.h"
}

// todo: maybe should support in guc
#define ORC_BUFFER_SIZE 1024 * 1024

namespace pax {
class OrcFileWriter {
 public:
  struct OrcWriterOptions {
    uint64_t stripe_size;
    uint32_t batch_row_size;
    int buffer_size;
    TupleDesc desc;
  };
  explicit OrcFileWriter(File* file,
                         const OrcFileWriter::OrcWriterOptions& writer_options);
  ~OrcFileWriter();
  uint64_t WriteSize() const { return out_->getLength(); }

  void Flush();

  void Close();

  void ParseTupleAndWrite(const TupleTableSlot* slot);

 private:
  OrcWriterOptions options_;
  uint32_t batch_row_index_;
  std::unique_ptr<orc::Writer> writer_;
  std::unique_ptr<orc::OutputStream> out_;
  std::unique_ptr<orc::ColumnVectorBatch> batch_;
  std::unique_ptr<orc::Type> type_;
  orc::DataBuffer<char> buffer_;
  uint64_t buffer_offset_;
};

class OrcFileOutputStream : public orc::OutputStream {
 public:
  explicit OrcFileOutputStream(File* file)
      : file_(std::move(file)), closed_(false) {}

  ~OrcFileOutputStream() { close(); }
  uint64_t getLength() const override { return bytes_written_; }

  uint64_t getNaturalWriteSize() const override { return ORC_BUFFER_SIZE; }

  void write(const void* buf, size_t length) override {
    if (length <= 0) {
      return;
    }
    size_t actual_written_size = 0;
    actual_written_size = this->file_->Write(buf, length);
    // todo should check whether the actual size is equal to length
    assert(actual_written_size == length);
  }

  const std::string& getName() const override { return file_->GetPath(); }

  void close() override {
    if (!closed_) {
      closed_ = true;
      file_->Flush();
      file_->Close();
    }
  }

 private:
  File* file_;
  uint64_t bytes_written_;
  bool closed_;
};

class OrcFileReader {
 public:
  struct OrcReaderOptions {
    uint32_t batch_row_size;
    TupleDesc desc;
  };

  explicit OrcFileReader(File* file);
  ~OrcFileReader();
  bool ReadNextBatch(CTupleSlot* slot);

  void Close() {}

 private:
  void readTupleFromBatchVector(Datum* datum, bool* isnull, int i, uint64_t row,
                                TupleDesc desc);

  std::unique_ptr<orc::Reader> reader_;
  std::unique_ptr<orc::InputStream> in_;
  std::unique_ptr<orc::ColumnVectorBatch> batch_;
  std::unique_ptr<orc::RowReader> row_reader_;
  uint64_t batch_row_index_;
  uint64_t batch_row_size_;
};

class OrcFileInputStream : public orc::InputStream {
 public:
  explicit OrcFileInputStream(File* file)
      : file_(std::move(file)), closed_(false) {}

  ~OrcFileInputStream() { close(); }

  uint64_t getLength() const override { return file_->GetSize(); }

  uint64_t getNaturalReadSize() const override { return ORC_BUFFER_SIZE; }

  void read(void* buf, size_t length, size_t offset) override {
    if (length <= 0) {
      return;
    }
    size_t actual_read_size = 0;
    actual_read_size = this->file_->Read(buf, length, offset);
    // todo  whether should check whether the actual size is equal to length
    assert(actual_read_size == length);
  }

  const std::string& getName() const override { return file_->GetPath(); };

  void close() {
    if (!closed_) {
      closed_ = true;
      file_->Close();
    }
  }

 private:
  File* file_;
  bool closed_;
};

}  // namespace pax
