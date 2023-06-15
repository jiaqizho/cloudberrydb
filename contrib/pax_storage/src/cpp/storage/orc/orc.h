#pragma once

#include <string>
#include <utility>
#include <vector>

// The libproto defined `FATAL` inside as a marco linker
#undef FATAL
#include "storage/orc/orc_proto.pb.h"
#include "storage/orc/protobuf_stream.h"
#define FATAL 22

#include "catalog/micro_partition_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_defer.h"
#include "exceptions/CException.h"
#include "storage/micro_partition.h"
#include "storage/pax_column.h"

namespace pax {

#define VERIFY_ORC 1
#define ORC_MAGIC_ID "ORC"
#define ORC_MAGIC_ID_LEN 3
// ORC cpp writer
#define ORC_WRITER_ID 1
#define ORC_SOFT_VERSION "1"
#define ORC_FILE_MAJOR_VERSION 1
#define ORC_WRITER_VERSION 1
#define ORC_POST_SCRIPT_SIZE 1

class OrcWriter : public MicroPartitionWriter {
 public:
  ~OrcWriter() {
    delete pax_columns_;
    delete file_;
  }

  void Flush() override;

  void WriteTuple(CTupleSlot *slot) override;

  void WriteTupleN(CTupleSlot **slot, size_t n) override;

  void Close() override;

  size_t EstimatedSize() const override;

  const std::string FullFileName() const override;

  // TODO(jiaqizho): using pg type mapping to replace fixed one
  // typlen + typname -> orc type id
  //
  // std::map<std::pair<int, const char *>, ::orc::proto::Type_Kind>
  // type_mapping = {
  //     {{-1, ""}, ::orc::proto::Type_Kind_STRING},
  //     {{-1, "bytea"}, ::orc::proto::Type_Kind_BYTE},
  //     {{1, "char"}, ::orc::proto::Type_Kind_CHAR},
  //     {{1, "bool"}, ::orc::proto::Type_Kind_BOOLEAN},
  //     {{2, "int2"}, ::orc::proto::Type_Kind_SHORT},
  //     {{4, "int4"}, ::orc::proto::Type_Kind_INT},
  //     {{4, "float4"}, ::orc::proto::Type_Kind_FLOAT},
  //     {{4, "date"}, ::orc::proto::Type_Kind_DATE},
  //     {{8, "float8"}, ::orc::proto::Type_Kind_DATE},
  //     {{8, "timestamp"}, ::orc::proto::Type_Kind_TIMESTAMP},
  // };
  static inline std::vector<orc::proto::Type_Kind> BuildSchema(TupleDesc desc) {
    std::vector<orc::proto::Type_Kind> type_kinds;
    for (int i = 0; i < desc->natts; i++) {
      auto *attr = &desc->attrs[i];
      if (attr->attbyval) {
        switch (attr->attlen) {
          case 1:
            type_kinds.emplace_back(orc::proto::Type_Kind::Type_Kind_BYTE);
            break;
          case 2:
            type_kinds.emplace_back(orc::proto::Type_Kind::Type_Kind_SHORT);
            break;
          case 4:
            type_kinds.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
            break;
          case 8:
            type_kinds.emplace_back(orc::proto::Type_Kind::Type_Kind_LONG);
            break;
          default:
            Assert(!"should not be here! pg_type which attbyval=true only have typlen of "
                  "1, 2, 4, or 8");
        }
      } else {
        Assert(attr->attlen > 0 || attr->attlen == -1);
        type_kinds.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
      }
    }

    return type_kinds;
  }

  // TODO(jiaqizho): split into orc_factory.h
  static MicroPartitionWriter *CreateWriter(
      FileSystem *fs, const MicroPartitionWriter::WriterOptions options) {
    File *file_ = fs->Open(options.file_name);
    Assert(file_ != nullptr);
    auto types = BuildSchema(options.desc);

    return CreateWriter(file_, std::move(types), options);
  }

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif
  static MicroPartitionWriter *CreateWriter(
      File *file, const std::vector<orc::proto::Type_Kind> column_types,
      const MicroPartitionWriter::WriterOptions options) {
    return new OrcWriter(options, column_types, file);
  }

  // after create a new writer or old stripe have been flushed
  // stripe_info_ in memory should reinit
  void InitStripe();

  void BuildFooterType();
  bool WriteStripe(BufferedOutputStream *buffer_mem_stream);
  void WriteMetadata(BufferedOutputStream *buffer_mem_stream);
  void WriteFileFooter(BufferedOutputStream *buffer_mem_stream);
  void WritePostscript(BufferedOutputStream *buffer_mem_stream);

  OrcWriter(const MicroPartitionWriter::WriterOptions &orc_writer_options,
            const std::vector<orc::proto::Type_Kind> column_types, File *file);

 protected:
  PaxColumns *pax_columns_;
  const std::vector<orc::proto::Type_Kind> column_types_;
  File *file_;
  WriteSummary summary_;

  ::orc::proto::Footer file_footer_;
  ::orc::proto::PostScript post_script_;
  ::orc::proto::StripeInformation stripe_info_;
  ::orc::proto::Metadata meta_data_;

  uint64 stripe_rows_ = 0;
  uint64 total_rows_ = 0;
  uint64 current_offset_ = 0;
};

class OrcReader : public MicroPartitionReader {
 public:
  struct StripeInformation {
    uint64 footer_length_;
    uint64 data_length_;
    uint64 numbers_of_row_;
    uint64 offset_;

    uint64 index_length_;
    uint64 stripe_footer_start_;

    // refine column statistics if we do need it
    ::orc::proto::StripeStatistics stripe_statistics;
  };

  ~OrcReader() { delete file_; }

  StripeInformation *GetStripeInfo(size_t index) const;

  PaxColumns *ReadStripe(size_t index);

  size_t GetNumberOfStripes() const;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(CTupleSlot *cslot) override;

  void Seek(size_t offset) override;

  uint64 Offset() const override;

  size_t Length() const override;

  void SetFilter(Filter *filter) override;

  static MicroPartitionReader *CreateReader(File *file) {
    return new OrcReader(file);
  }

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  // there is an optimization here, in standard ORC, A single ORC
  // file will read
  // follow these step:
  // - read postscript size
  // - read post script
  // - read file footer
  // - read meta if exist
  // the footer information of a single ORC file needing cost 3-4 iops
  // consider add a new filed after postscript size, contain the full size of
  // footer information
  explicit OrcReader(File *file);

  void ReadMetadata(ssize_t file_length, uint64 post_script_len);

  void BuildProtoTypes();

  void ReadFooter(size_t footer_offset, size_t footer_len);

  void ReadPostScript(size_t file_size, uint64 post_script_len);

  // Clean up reading status
  void ResetCurrentReading();

  // Optional, when reused buffer is not set, new memory will be generated for
  // ReadTuple
  void SetReadBuffer(DataBuffer<char> *data_buffer) override;

 protected:
  std::vector<orc::proto::Type_Kind> column_types_;
  File *file_;

  DataBuffer<char> *reused_buffer_;
  PaxColumns *working_pax_columns_;
  size_t current_stripe_index_ = 0;
  size_t current_row_index_ = 0;
  uint64 current_offset_ = 0;

  uint32 *current_nulls_ = nullptr;

  orc::proto::PostScript post_script_;
  orc::proto::Footer file_footer_;
  orc::proto::Metadata meta_data_;

  size_t num_of_stripes;
};

class OrcIteratorReader : public MicroPartitionReader {
 public:
  explicit OrcIteratorReader(const FileSystemPtr &fs);

  virtual ~OrcIteratorReader();

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(CTupleSlot *slot) override;

  uint64 Offset() const override;

  void Seek(size_t offset) override;

  size_t Length() const override;

  void SetFilter(Filter *filter) override { reader_->SetFilter(filter); }

  void SetReadBuffer(DataBuffer<char> *data_buffer) override;

 private:
  std::string block_id_;
  MicroPartitionReader *reader_;
  DataBuffer<char> *reused_buffer_;
  bool closed_ = true;
};

};  // namespace pax
