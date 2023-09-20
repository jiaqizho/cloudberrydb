#pragma once

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_defer.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/proto/proto_wrappers.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
class MicroPartitionStats;

#define ORC_MAGIC_ID "ORC"
// ORC cpp writer
#define ORC_WRITER_ID 1
#define ORC_SOFT_VERSION "1"
#define ORC_FILE_MAJOR_VERSION 1
#define ORC_WRITER_VERSION 1
#define ORC_POST_SCRIPT_SIZE 1

class OrcWriter : public MicroPartitionWriter {
 public:
  OrcWriter(const MicroPartitionWriter::WriterOptions &orc_writer_options,
            const std::vector<orc::proto::Type_Kind> &column_types, File *file);

  ~OrcWriter() override;

  void Flush() override;

  void WriteTuple(CTupleSlot *slot) override;

  void WriteTupleN(CTupleSlot **slot, size_t n) override;

  void Close() override;

  MicroPartitionWriter *SetStatsCollector(
      MicroPartitionStats *mpstats) override;

  size_t PhysicalSize() const override;

  static std::vector<orc::proto::Type_Kind> BuildSchema(TupleDesc desc);

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  // only for test
  static MicroPartitionWriter *CreateWriter(
      MicroPartitionWriter::WriterOptions options,
      const std::vector<orc::proto::Type_Kind> column_types, File *file) {
    std::vector<std::tuple<ColumnEncoding_Kind, int>> all_no_encoding_types;
    for (auto _ : column_types) {
      (void)_;
      all_no_encoding_types.emplace_back(std::make_tuple(
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
    }

    options.encoding_opts = all_no_encoding_types;

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

#ifdef ENABLE_PLASMA
class PaxColumnCache;
#endif  // ENABLE_PLASMA

class OrcReader : public MicroPartitionReader {
 public:
  struct StripeInformation {
    uint64 footer_length;
    uint64 data_length;
    uint64 numbers_of_row;
    uint64 offset;

    uint64 index_length;
    uint64 stripe_footer_start;

    // refine column statistics if we do need it
    ::orc::proto::StripeStatistics stripe_statistics;
  };

  explicit OrcReader(File *file);

  ~OrcReader() override;

  StripeInformation *GetStripeInfo(size_t index) const;

  PaxColumns *ReadStripe(size_t index, bool *proj_map = nullptr,
                         size_t proj_len = 0);

  size_t GetNumberOfStripes() const;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(CTupleSlot *cslot) override;

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  PaxColumns *GetAllColumns() override;

  orc::proto::StripeFooter ReadStripeWithProjection(
      DataBuffer<char> *data_buffer, OrcReader::StripeInformation *stripe_info,
      const bool *proj_map, size_t proj_len);

  void ReadMetadata(ssize_t file_length, uint64 post_script_len);

  void BuildProtoTypes();

  void ReadFooter(size_t footer_offset, size_t footer_len);

  void ReadPostScript(size_t file_size, uint64 post_script_len);

  // Clean up reading status
  void ResetCurrentReading();

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

  size_t num_of_stripes_;
  bool *proj_map_;
  size_t proj_len_;

  bool is_close_;
#ifdef ENABLE_PLASMA
  PaxColumnCache *pax_column_cache_ = nullptr;
  std::vector<std::string> release_key_;
#endif  // ENABLE_PLASMA
};

};  // namespace pax
