#include "storage/orc/orc.h"

#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

#include "catalog/micro_partition_stats.h"
#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_int.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"
#include "storage/pax_filter.h"
namespace pax {

OrcWriter::OrcWriter(
    const MicroPartitionWriter::WriterOptions &orc_writer_options,
    const std::vector<orc::proto::Type_Kind> &column_types,
    const std::vector<ColumnEncoding_Kind> &column_encoding_types, File *file)
    : MicroPartitionWriter(orc_writer_options),
      column_types_(column_types),
      column_encoding_types_(column_encoding_types),
      file_(file),
      current_offset_(0) {
  pax_columns_ = new PaxColumns(column_types, column_encoding_types);

  summary_.rel_oid = orc_writer_options.rel_oid;
  summary_.block_id = orc_writer_options.block_id;
  summary_.file_name = orc_writer_options.file_name;

  file_footer_.set_headerlength(0);
  file_footer_.set_contentlength(0);
  file_footer_.set_numberofrows(0);
  file_footer_.set_rowindexstride(0);
  file_footer_.set_writer(ORC_WRITER_ID);
  file_footer_.set_softwareversion(ORC_SOFT_VERSION);
  BuildFooterType();

  post_script_.set_footerlength(0);
  post_script_.set_compression(orc::proto::CompressionKind::NONE);
  post_script_.set_compressionblocksize(0);

  post_script_.add_version(ORC_FILE_MAJOR_VERSION);
  post_script_.set_writerversion(ORC_WRITER_VERSION);
  post_script_.set_magic(ORC_MAGIC_ID);

  InitStripe();
}

OrcWriter::~OrcWriter() {
  delete pax_columns_;
  delete file_;
}

MicroPartitionWriter *OrcWriter::SetStatsCollector(MicroPartitionStats *mpstats) {
  if (mpstats)
    mpstats->SetStatsMessage(&summary_.mp_stats, column_types_.size());
  return MicroPartitionWriter::SetStatsCollector(mpstats);
}

void OrcWriter::Flush() {
  BufferedOutputStream buffer_mem_stream(nullptr, 2048);
  if (WriteStripe(&buffer_mem_stream)) {
    Assert(current_offset_ >= buffer_mem_stream.GetDataBuffer()->Used());
    file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                   buffer_mem_stream.GetDataBuffer()->Used(),
                   current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());
    file_->Flush();
    pax_columns_->Clear();
  }
}

void OrcWriter::WriteTuple(CTupleSlot *slot) {
  int n;
  TupleTableSlot *table_slot;
  TupleDesc table_desc;
  int16 type_len;
  bool type_by_val;

  summary_.num_tuples++;

  table_slot = slot->GetTupleTableSlot();
  table_desc = slot->GetTupleDesc();
  n = table_desc->natts;

  CBDB_CHECK(pax_columns_->GetColumns() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);

  pax_columns_->AddRows(1);

  for (int i = 0; i < n; i++) {
    type_len = table_desc->attrs[i].attlen;
    type_by_val = table_desc->attrs[i].attbyval;

    AssertImply(table_desc->attrs[i].attisdropped, table_slot->tts_isnull[i]);

    if (table_slot->tts_isnull[i]) {
      (*pax_columns_)[i]->AppendNull();
      continue;
    }

    if (type_by_val) {
      switch (type_len) {
        case 1: {
          auto value = cbdb::DatumToInt8(table_slot->tts_values[i]);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 2: {
          auto value = cbdb::DatumToInt16(table_slot->tts_values[i]);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 4: {
          auto value = cbdb::DatumToInt32(table_slot->tts_values[i]);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 8: {
          auto value = cbdb::DatumToInt64(table_slot->tts_values[i]);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        default:
          Assert(!"should not be here! pg_type which attbyval=true only have typlen of "
                  "1, 2, 4, or 8 ");
      }
    } else {
      switch (type_len) {
        case -1: {
          void *vl = nullptr;
          int len = -1;
          vl = cbdb::PointerAndLenFromDatum(table_slot->tts_values[i], &len);
          Assert(vl != nullptr && len != -1);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(vl), len);
          break;
        }
        default:
          Assert(type_len > 0);
          (*pax_columns_)[i]->Append(static_cast<char *>(cbdb::DatumToPointer(
                                         table_slot->tts_values[i])),
                                     type_len);
          break;
      }
    }
  }
}

void OrcWriter::WriteTupleN(CTupleSlot **slot, size_t n) {
  // TODO(jiaqizho): support WriteTupleN
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream) {
  std::vector<orc::proto::Stream> streams;
  std::vector<ColumnEncoding> encoding_kinds;
  orc::proto::StripeFooter stripe_footer;
  auto stripe_stats = meta_data_.add_stripestats();

  size_t data_len = 0;
  size_t number_of_row = 0;

  number_of_row = pax_columns_->GetRows();
  stripe_rows_ = number_of_row;

  // No need add stripe if nothing in memeory
  if (number_of_row == 0) {
    return false;
  }

  PaxColumns::ColumnStreamsFunc column_streams_func =
      [&streams](const orc::proto::Stream_Kind &kind, size_t column,
                 size_t length) {
        orc::proto::Stream stream;
        stream.set_kind(kind);
        stream.set_column(static_cast<uint32>(column));
        stream.set_length(length);
        streams.push_back(std::move(stream));
      };

  PaxColumns::ColumnEncodingFunc column_encoding_func =
      [&encoding_kinds](const ColumnEncoding_Kind &encoding_kind,
                        size_t origin_len) {
        ColumnEncoding column_encoding;
        column_encoding.set_kind(encoding_kind);
        column_encoding.set_length(origin_len);

        encoding_kinds.push_back(std::move(column_encoding));
      };

  DataBuffer<char> *data_buffer =
      pax_columns_->GetDataBuffer(column_streams_func, column_encoding_func);

  for (const auto &stream : streams) {
    *stripe_footer.add_streams() = stream;
    data_len += stream.length();
  }

  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
    auto pb_stats = stripe_stats->add_colstats();
    PaxColumn *pax_column = (*pax_columns_)[i];

    *stripe_footer.add_pax_col_encodings() = encoding_kinds[i];

    pb_stats->set_hasnull(pax_column->HasNull());
    pb_stats->set_numberofvalues(pax_column->GetRows());
  }

  stripe_footer.set_writertimezone("GMT");
  buffer_mem_stream->Set(data_buffer, 2048);

  // check memory io with protobuf
  CBDB_CHECK(stripe_footer.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  stripe_info_.set_indexlength(0);
  stripe_info_.set_datalength(data_len);
  stripe_info_.set_footerlength(buffer_mem_stream->GetSize());
  stripe_info_.set_numberofrows(stripe_rows_);

  *file_footer_.add_stripes() = stripe_info_;

  current_offset_ += buffer_mem_stream->GetSize();
  total_rows_ += stripe_rows_;

  // reset the stripe
  InitStripe();
  return true;
}

void OrcWriter::Close() {
  BufferedOutputStream buffer_mem_stream(nullptr, 2048);
  size_t file_offset = current_offset_;
  bool not_empty_stripe = false;
  DataBuffer<char> *data_buffer;

  not_empty_stripe = WriteStripe(&buffer_mem_stream);
  if (!not_empty_stripe) {
    data_buffer = new DataBuffer<char>(2048);
    buffer_mem_stream.Set(data_buffer, 2048);
  }

  WriteMetadata(&buffer_mem_stream);
  WriteFileFooter(&buffer_mem_stream);
  WritePostscript(&buffer_mem_stream);
  if (summary_callback_) {
    summary_.file_size = buffer_mem_stream.GetDataBuffer()->Used();
    summary_callback_(summary_);
  }

  file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                 buffer_mem_stream.GetDataBuffer()->Used(), file_offset);
  file_->Flush();
  file_->Close();
  if (!not_empty_stripe) {
    delete data_buffer;
  }
}

size_t OrcWriter::EstimatedSize() const {
  return pax_columns_->EstimatedSize();
}

void OrcWriter::InitStripe() {
  stripe_info_.set_offset(current_offset_);
  stripe_info_.set_indexlength(0);
  stripe_info_.set_datalength(0);
  stripe_info_.set_footerlength(0);
  stripe_info_.set_numberofrows(0);

  stripe_rows_ = 0;
}

void OrcWriter::BuildFooterType() {
  auto proto_type = file_footer_.add_types();
  proto_type->set_maximumlength(0);
  proto_type->set_precision(0);
  proto_type->set_scale(0);
  proto_type->set_kind(::orc::proto::Type_Kind_STRUCT);

  // TODO(jiaqizho): support interface for meta kv, if we do need this function
  //   protoAttr->set_key(key);
  //   protoAttr->set_value(value);
  for (size_t i = 0; i < column_types_.size(); ++i) {
    auto orc_type = column_types_[i];

    auto sub_proto_type = file_footer_.add_types();
    sub_proto_type->set_maximumlength(0);
    sub_proto_type->set_precision(0);
    sub_proto_type->set_scale(0);
    sub_proto_type->set_kind(orc_type);

    file_footer_.mutable_types(0)->add_subtypes(i);
  }
}

void OrcWriter::WriteMetadata(BufferedOutputStream *buffer_mem_stream) {
  buffer_mem_stream->StartBufferOutRecord();
  CBDB_CHECK(meta_data_.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  post_script_.set_metadatalength(buffer_mem_stream->EndBufferOutRecord());
}

void OrcWriter::WriteFileFooter(BufferedOutputStream *buffer_mem_stream) {
  file_footer_.set_contentlength(current_offset_ - file_footer_.headerlength());
  file_footer_.set_numberofrows(total_rows_);

  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
    auto pb_stats = file_footer_.add_statistics();
    // FIXME(jiaqizho): the statistics in file footer is not accurate
    // but statistics in stripe stats is accurate
    pb_stats->set_hasnull(false);
    pb_stats->set_numberofvalues((*pax_columns_)[i]->GetRows());
  }

  buffer_mem_stream->StartBufferOutRecord();
  CBDB_CHECK(file_footer_.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  post_script_.set_footerlength(buffer_mem_stream->EndBufferOutRecord());
}

void OrcWriter::WritePostscript(BufferedOutputStream *buffer_mem_stream) {
  buffer_mem_stream->StartBufferOutRecord();
  CBDB_CHECK(post_script_.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  char ps_len = static_cast<char>(buffer_mem_stream->EndBufferOutRecord());
  buffer_mem_stream->DirectWrite(&ps_len, sizeof(unsigned char));
}

OrcReader::OrcReader(File *file)
    : file_(file),
      reused_buffer_(nullptr),
      working_pax_columns_(nullptr),
      num_of_stripes_(0),
      proj_map_(nullptr) {
  size_t file_length = file_->FileLength();
  uint64 post_script_len = 0;

  file_->PRead(&post_script_len, ORC_POST_SCRIPT_SIZE,
               (off_t)(file_length - ORC_POST_SCRIPT_SIZE));

  ReadPostScript(file_length, post_script_len);

  size_t footer_len = post_script_.footerlength();
  size_t tail_len = ORC_POST_SCRIPT_SIZE + post_script_len + footer_len;
  size_t footer_offset = file_length - tail_len;

  ReadFooter(footer_offset, footer_len);
  num_of_stripes_ = file_footer_.stripes_size();

  if (post_script_.metadatalength() != 0)
    ReadMetadata(file_length, post_script_len);
}

OrcReader::~OrcReader() { delete file_; }

void OrcReader::ReadMetadata(ssize_t file_length, uint64 post_script_len) {
  uint64 meta_len = post_script_.metadatalength();
  uint64 footer_len = post_script_.footerlength();
  off_t meta_start = file_length - meta_len - footer_len - post_script_len -
                     ORC_POST_SCRIPT_SIZE;
  char read_buffer[meta_len];
  SeekableInputStream input_stream(read_buffer, meta_len);

  Assert(meta_start >= 0);
  file_->PReadN(read_buffer, meta_len, meta_start);

  CBDB_CHECK(meta_data_.ParseFromZeroCopyStream(&input_stream),
             cbdb::CException::ExType::kExTypeIOError);
}

void OrcReader::BuildProtoTypes() {
  int max_id = 0;

  max_id = file_footer_.types_size();

  CBDB_CHECK(max_id > 0, cbdb::CException::ExType::kExTypeInvalidORCFormat);

  const orc::proto::Type &type = file_footer_.types(0);

  // There is an assumption here: for all pg tables, the outermost structure
  // should be Type_Kind_STRUCT
  CBDB_CHECK(type.kind() == orc::proto::Type_Kind_STRUCT,
             cbdb::CException::ExType::kExTypeInvalidORCFormat);
  CBDB_CHECK(type.subtypes_size() == max_id - 1,
             cbdb::CException::ExType::kExTypeInvalidORCFormat);

  for (int j = 0; j < type.subtypes_size(); ++j) {
    int sub_type_id = static_cast<int>(type.subtypes(j)) + 1;
    const orc::proto::Type &sub_type = file_footer_.types(sub_type_id);
    // should allow struct contain struct
    // but not support yet
    CBDB_CHECK(sub_type.kind() != orc::proto::Type_Kind_STRUCT,
               cbdb::CException::ExType::kExTypeInvalidORCFormat);

    column_types_.emplace_back(sub_type.kind());
  }
}

void OrcReader::ReadFooter(size_t footer_offset, size_t footer_len) {
  char buffer[footer_len];

  file_->PReadN(&buffer, footer_len, footer_offset);

  SeekableInputStream input_stream(buffer, footer_len);
  CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
             cbdb::CException::ExType::kExTypeIOError);

  BuildProtoTypes();
  current_nulls_ = new uint32[column_types_.size()];
  memset(current_nulls_, 0, column_types_.size() * sizeof(uint32));
}

void OrcReader::ReadPostScript(size_t file_size, uint64 post_script_len) {
  char post_script_buffer[post_script_len];
  off_t offset;

  offset = (off_t)(file_size - ORC_POST_SCRIPT_SIZE - post_script_len);
  Assert(offset >= 0);

  file_->PReadN(post_script_buffer, post_script_len, offset);

  post_script_.ParseFromArray(&post_script_buffer,
                              static_cast<int>(post_script_len));
  // TODO(jiaqizho): verify orc format here
}

static bool ProjShouldReadAll(const bool *const proj_map, size_t proj_len) {
  if (!proj_map) {
    return true;
  }

  for (size_t i = 0; i < proj_len; i++) {
    if (!proj_map[i]) {
      return false;
    }
  }

  return true;
}

orc::proto::StripeFooter OrcReader::ReadStripeWithProjection(
    DataBuffer<char> *data_buffer, OrcReader::StripeInformation *stripe_info,
    const bool *const proj_map, size_t proj_len) {
  size_t stripe_footer_offset = 0;
  orc::proto::StripeFooter stripe_footer;
  size_t streams_index = 0;
  uint64_t batch_len = 0;
  uint64_t batch_offset = 0;
  size_t index = 0;

  stripe_footer_offset = stripe_info->data_length + stripe_info->index_length;

  /* Check all column projection is true.
   * If no need do column projection, read all
   * buffer(data + stripe footer) from stripe and decode stripe footer.
   */
  if (ProjShouldReadAll(proj_map, proj_len)) {
    file_->PReadN(data_buffer->GetBuffer(), stripe_info->footer_length,
                  stripe_info->offset);
    SeekableInputStream input_stream(
        data_buffer->GetBuffer() + stripe_footer_offset,
        stripe_info->footer_length - stripe_footer_offset);
    if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
      // fail to do memory io with protobuf
      CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
    }

    return stripe_footer;
  }

  Assert(stripe_info->index_length == 0);

  /* If need do column projection here
   * Then read stripe footer and decode it before read data part
   */
  file_->PReadN(data_buffer->GetBuffer(),
                stripe_info->footer_length - stripe_footer_offset,
                stripe_info->offset + stripe_footer_offset);

  SeekableInputStream input_stream(
      data_buffer->GetBuffer(),
      stripe_info->footer_length - stripe_footer_offset);

  if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
  }

  data_buffer->BrushBackAll();

  batch_offset = stripe_info->offset;

  while (index < column_types_.size()) {
    // Current column have been skipped
    // Move `batch_offset` and `streams_index` to the right position
    if (!proj_map[index]) {
      index++;

      const orc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
        batch_offset += n_stream->length();
      } while (n_stream->kind() != ::orc::proto::Stream_Kind::Stream_Kind_DATA);

      continue;
    }

    batch_len = 0;

    /* Current column should be read
     * In this case, did a greedy algorithm to combine io: while
     * the current column is being read, it is necessary
     * to ensure that subsequent columns will be read in the same io.
     *
     * So in `do...while`, only the `batch_size` which io needs to read
     * is calculated, until meet a column which needs to be skipped.
     */
    do {
      bool has_null = stripe_info->stripe_statistics.colstats(index).hasnull();
      if (has_null) {
        const orc::proto::Stream &non_null_stream =
            stripe_footer.streams(streams_index++);
        batch_len += non_null_stream.length();
      }

      const orc::proto::Stream *len_or_data_stream =
          &stripe_footer.streams(streams_index++);
      batch_len += len_or_data_stream->length();

      if (len_or_data_stream->kind() ==
          ::orc::proto::Stream_Kind::Stream_Kind_LENGTH) {
        len_or_data_stream = &stripe_footer.streams(streams_index++);
        batch_len += len_or_data_stream->length();
      }
    } while ((++index) < column_types_.size() && proj_map[index]);

    file_->PReadN(data_buffer->GetAvailableBuffer(), batch_len, batch_offset);
    data_buffer->Brush(batch_len);
    batch_offset += batch_len;
  }

  return stripe_footer;
}

template <typename T>
static PaxColumn *GetIntEncodingColumn(DataBuffer<char> *data_buffer,
                                       const orc::proto::Stream &data_stream,
                                       const ColumnEncoding &data_encoding) {
  uint32 column_data_size = 0;
  uint64 column_data_len = 0;

  DataBuffer<T> *column_data_buffer = nullptr;
  PaxIntColumn<T> *pax_column = nullptr;

  column_data_size = static_cast<uint32>(data_stream.column());
  column_data_len = static_cast<uint64>(data_stream.length());

  column_data_buffer = new DataBuffer<T>(
      reinterpret_cast<T *>(data_buffer->GetAvailableBuffer()), column_data_len,
      false, false);
  column_data_buffer->BrushAll();

  data_buffer->Brush(column_data_len);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;

  if (data_encoding.kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    Assert(column_data_size == column_data_buffer->GetSize());
    pax_column = new PaxIntColumn<T>(std::move(decoding_option));
  } else {
    Assert(data_encoding.length() / sizeof(T) == column_data_size);
    pax_column =
        new PaxIntColumn<T>(column_data_size, std::move(decoding_option));
  }

  pax_column->Set(column_data_buffer);
  return pax_column;
}

PaxColumns *OrcReader::ReadStripe(size_t index, bool *proj_map,
                                  size_t proj_len) {
  auto stripe_info = GetStripeInfo(index);
  auto pax_columns = new PaxColumns();
  DataBuffer<char> *data_buffer = nullptr;
  orc::proto::StripeFooter stripe_footer;
  size_t streams_index = 0;
  size_t streams_size = 0;
  size_t encoding_kinds_size = 0;

  Assert(stripe_info->index_length == 0);
  pax_columns->AddRows(stripe_info->numbers_of_row);

  DEFER({ delete stripe_info; });

  if (unlikely(stripe_info->footer_length == 0)) {
    return pax_columns;
  }

  if (reused_buffer_) {
    while (reused_buffer_->Capacity() < stripe_info->footer_length) {
      reused_buffer_->ReSize(reused_buffer_->Capacity() / 2 * 3);
    }
    data_buffer = new DataBuffer<char>(
        reused_buffer_->GetBuffer(), reused_buffer_->Capacity(), false, false);

  } else {
    data_buffer = new DataBuffer<char>(stripe_info->footer_length);
  }
  pax_columns->Set(data_buffer);

  /* `ReadStripeWithProjection` will read the column memory which filter by
   * `proj_map`, and initialize `stripe_footer`
   *
   * Notice that: should catch `kExTypeIOError` then delete pax columns
   * But for now we will destroy memory context if exception happen.
   * And we don't have a decision that should we use `try...catch` at yet,
   * so it's ok that we just no catch here.
   */
  stripe_footer =
      ReadStripeWithProjection(data_buffer, stripe_info, proj_map, proj_len);

  streams_size = stripe_footer.streams_size();
  encoding_kinds_size = stripe_footer.pax_col_encodings_size();

  if (unlikely(streams_size == 0 && column_types_.empty())) {
    return pax_columns;
  }

  data_buffer->BrushBackAll();

  AssertImply(proj_len != 0, column_types_.size() <= proj_len);
  Assert(encoding_kinds_size <= column_types_.size());

  for (size_t index = 0; index < column_types_.size(); index++) {
    /* Skip read current column, just move `streams_index` after
     * `Stream_Kind_DATA` but still need append nullptr into `PaxColumns` to
     * make sure sizeof pax_columns eq with column number
     */
    if (proj_map && !proj_map[index]) {
      const orc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
      } while (n_stream->kind() != ::orc::proto::Stream_Kind::Stream_Kind_DATA);

      pax_columns->Append(nullptr);
      continue;
    }

    DataBuffer<bool> *non_null_bitmap = nullptr;
    bool has_null = stripe_info->stripe_statistics.colstats(index).hasnull();
    if (has_null) {
      uint64 non_null_length = 0;
      const orc::proto::Stream &non_null_stream =
          stripe_footer.streams(streams_index++);
      non_null_length = static_cast<uint32>(non_null_stream.length());

      non_null_bitmap = new DataBuffer<bool>(
          reinterpret_cast<bool *>(data_buffer->GetAvailableBuffer()),
          non_null_length, false, false);
      non_null_bitmap->BrushAll();
      data_buffer->Brush(non_null_length);
    }

    switch (column_types_[index]) {
      case (orc::proto::Type_Kind::Type_Kind_STRING): {
        uint32 column_lens_size = 0;
        uint64 column_lens_len = 0;
        uint64 column_data_len = 0;
        DataBuffer<int64> *column_len_buffer = nullptr;
        DataBuffer<char> *column_data_buffer = nullptr;
        PaxNonFixedEncodingColumn *pax_column = nullptr;

        const orc::proto::Stream &len_stream =
            stripe_footer.streams(streams_index++);
        const orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);

        column_lens_size = static_cast<uint32>(len_stream.column());
        column_lens_len = static_cast<uint64>(len_stream.length());

        column_len_buffer = new DataBuffer<int64>(
            reinterpret_cast<int64 *>(data_buffer->GetAvailableBuffer()),
            column_lens_len, false, false);
        column_len_buffer->BrushAll();
        data_buffer->Brush(column_lens_len);

        column_data_len = data_stream.length();

#ifdef ENBALE_DEBUG
        if (data_encoding.kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          size_t segs_size = 0;
          for (size_t i = 0; i < column_len_buffer->GetSize(); i++) {
            segs_size += (*column_len_buffer)[i];
          }
          Assert(column_data_len == segs_size);
        }
#endif

        column_data_buffer = new DataBuffer<char>(
            data_buffer->GetAvailableBuffer(), column_data_len, false, false);
        column_data_buffer->BrushAll();
        data_buffer->Brush(column_data_len);

        Assert(static_cast<uint32>(data_stream.column()) == column_lens_size);

        PaxDecoder::DecodingOption decoding_option;
        decoding_option.column_encode_type = data_encoding.kind();
        decoding_option.is_sign = true;

        if (data_encoding.kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          Assert(column_data_len == column_data_buffer->GetSize());
          pax_column =
              new PaxNonFixedEncodingColumn(0, std::move(decoding_option));
        } else {
          pax_column = new PaxNonFixedEncodingColumn(
              data_encoding.length(), std::move(decoding_option));
        }

        // current memory will be freed in pax_columns->data_
        pax_column->Set(column_data_buffer, column_len_buffer, column_data_len);
        pax_column->SetMemTakeOver(false);
        pax_columns->Append(pax_column);
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_BOOLEAN):
      case (orc::proto::Type_Kind::Type_Kind_BYTE): {
        const orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        uint32 column_data_size = 0;
        uint64 column_data_len = 0;
        DataBuffer<char> *column_data_buffer = nullptr;
        PaxCommColumn<char> *pax_column = nullptr;

        column_data_size = static_cast<uint32>(data_stream.column());
        column_data_len = static_cast<uint64>(data_stream.length());
        column_data_buffer = new DataBuffer<char>(
            reinterpret_cast<char *>(data_buffer->GetAvailableBuffer()),
            column_data_len, false, false);

        column_data_buffer->BrushAll();
        data_buffer->Brush(column_data_len);

        Assert(column_data_size == column_data_buffer->GetSize());
        pax_column = new PaxCommColumn<char>();
        pax_column->Set(column_data_buffer);
        pax_columns->Append(pax_column);
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_SHORT): {
        const orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);
        pax_columns->Append(GetIntEncodingColumn<int16>(
            data_buffer, data_stream, data_encoding));
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_INT): {
        const orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);
        pax_columns->Append(GetIntEncodingColumn<int32>(
            data_buffer, data_stream, data_encoding));
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_LONG): {
        const orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);
        pax_columns->Append(GetIntEncodingColumn<int64>(
            data_buffer, data_stream, data_encoding));
        break;
      }
      default:
        // should't be here
        Assert(!"should't be here, non-implemented type");
        break;
    }

    // fill nulls data buffer
    if (has_null) {
      Assert(pax_columns->GetColumns() > 0 && non_null_bitmap);
      auto last_column = (*pax_columns)[pax_columns->GetColumns() - 1];
      last_column->SetNulls(non_null_bitmap);
    }
  }

  Assert(streams_size == streams_index);
  return pax_columns;
}

OrcReader::StripeInformation *OrcReader::GetStripeInfo(size_t index) const {
  auto *stripe_info_in_mem = new StripeInformation();
  orc::proto::StripeInformation stripe_info;

  CBDB_CHECK(index < num_of_stripes_,
             cbdb::CException::ExType::kExTypeLogicError);

  stripe_info = file_footer_.stripes(static_cast<int>(index));
  stripe_info_in_mem->footer_length = stripe_info.footerlength();
  stripe_info_in_mem->data_length = stripe_info.datalength();
  stripe_info_in_mem->numbers_of_row = stripe_info.numberofrows();
  stripe_info_in_mem->offset = stripe_info.offset();

  stripe_info_in_mem->index_length = stripe_info.indexlength();
  stripe_info_in_mem->stripe_footer_start = stripe_info.offset() +
                                            stripe_info.indexlength() +
                                            stripe_info.datalength();

  stripe_info_in_mem->stripe_statistics =
      meta_data_.stripestats(static_cast<int>(index));

  return stripe_info_in_mem;
}

size_t OrcReader::GetNumberOfStripes() const { return num_of_stripes_; }

void OrcReader::Open(const ReaderOptions &options) {
  if (options.filter) proj_map_ = options.filter->GetColumnProjection();
}

void OrcReader::ResetCurrentReading() {
  if (working_pax_columns_) {
    delete working_pax_columns_;
    working_pax_columns_ = nullptr;
  }
  current_stripe_index_ = 0;
  current_row_index_ = 0;
  current_offset_ = 0;
  memset(current_nulls_, 0, column_types_.size() * sizeof(uint32));
}

void OrcReader::Close() {
  Assert(current_nulls_);
  ResetCurrentReading();
  delete[] current_nulls_;
  current_nulls_ = nullptr;
  file_->Close();
}

bool OrcReader::ReadTuple(CTupleSlot *cslot) {
  size_t row_nums = 0;
  TupleTableSlot *slot;
  size_t column_numbers = 0;
  size_t index = 0;
  size_t nattrs = 0;
  AttrMissing *attrmiss = nullptr;

  slot = cslot->GetTupleTableSlot();

  while (true) {
    nattrs = static_cast<size_t>(slot->tts_tupleDescriptor->natts);

    if (!working_pax_columns_) {
      // no data remain
      if (current_stripe_index_ >= GetNumberOfStripes()) {
        return false;
      }

      working_pax_columns_ =
          ReadStripe(current_stripe_index_++, proj_map_, nattrs);
      current_row_index_ = 0;
      for (size_t i = 0; i < column_types_.size(); i++) {
        current_nulls_[i] = 0;
      }
    }

    column_numbers = working_pax_columns_->GetColumns();

    // The column number in Pax file meta could be smaller than the column
    // number in TupleSlot in case after alter table add column DDL operation
    // was done.
    if (column_numbers > nattrs) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeSchemaNotMatch);
    }

    row_nums = working_pax_columns_->GetRows();

    // skip the empty stripe or current stripe already consumed
    if (unlikely(row_nums == 0) || current_row_index_ == row_nums) {
      delete working_pax_columns_;
      working_pax_columns_ = nullptr;
    } else {
      break;
    }
  }

  char *buffer = nullptr;
  size_t buffer_len = 0;

  //  first check if column has missing value
  if (slot->tts_tupleDescriptor->constr)
    attrmiss = slot->tts_tupleDescriptor->constr->missing;

  for (index = 0; index < nattrs; index++) {
    if (proj_map_ && !proj_map_[index]) {
      continue;
    }

    // handle PAX columns number inconsistent with pg catalog nattrs in case
    // data not been inserted yet or read pax file conserved before last add
    // column DDL is done, for these cases it is normal that pg catalog schema
    // is not match with that in PAX file:
    // 1. if atthasmissing is set, then return default column value.
    // 2. if atthasmissing is not set, then return null value.
    if (index >= column_numbers) {
      slot->tts_isnull[index] = true;
      //  The attrmiss default value memory is managed in CacheMemoryContext,
      //  which was allocated in RelationBuildTupleDesc.
      if (attrmiss && (slot->tts_tupleDescriptor->attrs[index].atthasmissing &&
                       attrmiss[index].am_present)) {
        slot->tts_values[index] = attrmiss[index].am_value;
        slot->tts_isnull[index] = false;
      }
      continue;
    }

    // In case column is droped, then set its value as null without reading data
    // tuples.
    if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
      slot->tts_isnull[index] = true;
      continue;
    }

    PaxColumn *column = ((*working_pax_columns_)[index]);

    // set default is not null
    slot->tts_isnull[index] = false;
    if (column->HasNull()) {
      auto null_bitmap = column->GetNulls();
      if (!(*null_bitmap)[current_row_index_]) {
        slot->tts_isnull[index] = true;
        current_nulls_[index]++;
        continue;
      }
    }

    Assert(current_row_index_ >= current_nulls_[index]);

    std::tie(buffer, buffer_len) =
        column->GetBuffer(current_row_index_ - current_nulls_[index]);
    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        slot->tts_values[index] = PointerGetDatum(buffer);
        break;
      }
      case kTypeFixed: {
        // FIXME(gongxun): get value info from PaxColumn
        switch (slot->tts_tupleDescriptor->attrs[index].attlen) {
          case 1:
            slot->tts_values[index] =
                cbdb::Int8ToDatum(*reinterpret_cast<int8 *>(buffer));
            break;
          case 2:
            slot->tts_values[index] =
                cbdb::Int16ToDatum(*reinterpret_cast<int16 *>(buffer));
            break;
          case 4:
            slot->tts_values[index] =
                cbdb::Int32ToDatum(*reinterpret_cast<int32 *>(buffer));
            break;
          case 8:
            slot->tts_values[index] =
                cbdb::Int64ToDatum(*reinterpret_cast<int64 *>(buffer));
            break;
          default:
            Assert(!"should't be here, fixed type len should be 1, 2, 4, 8");
        }
        break;
      }
      default: {
        Assert(!"should't be here, non-implemented column type in memory");
        break;
      }
    }
  }

  current_row_index_++;
  current_offset_++;
  cslot->SetOffset(current_offset_);

  return true;
}

uint64 OrcReader::Offset() const { return current_offset_; }

void OrcReader::SetReadBuffer(DataBuffer<char> *reused_buffer) {
  Assert(!reused_buffer_);
  CBDB_CHECK(reused_buffer->IsMemTakeOver(),
             cbdb::CException::ExType::kExTypeLogicError);
  reused_buffer->BrushBackAll();

  reused_buffer_ = reused_buffer;
}

size_t OrcReader::Length() const { return 0; }

OrcIteratorReader::OrcIteratorReader(FileSystem *fs)
    : MicroPartitionReader(fs), reader_(nullptr), reused_buffer_(nullptr) {}

void OrcIteratorReader::Open(const ReaderOptions &options) {
  File *file = file_system_->Open(options.file_name);
  Assert(file != nullptr);
  // last file should closed
  Assert(reader_ == nullptr);

  reader_ = OrcReader::CreateReader(file);
  if (reused_buffer_) {
    reader_->SetReadBuffer(reused_buffer_);
  }
  // TODO(jiaqizho): should remove OrcIteratorReader
  // and create micro partition writer/reader by a builder.
  // Then we won't pass the options to anywhere
  reader_->Open(options);
  closed_ = false;
}

void OrcIteratorReader::Close() {
  if (closed_) {
    return;
  }
  Assert(reader_);
  reader_->Close();
  delete reader_;
  reader_ = nullptr;
  closed_ = true;
}

uint64 OrcIteratorReader::Offset() const {
  Assert(reader_);
  return reader_->Offset();
}

bool OrcIteratorReader::ReadTuple(CTupleSlot *slot) {
  Assert(reader_);
  return reader_->ReadTuple(slot);
}

size_t OrcIteratorReader::Length() const {
  // TODO(gongxun): get length from orc file
  Assert(!"not implemented");
  return 0;
}

void OrcIteratorReader::SetReadBuffer(DataBuffer<char> *reused_buffer) {
  Assert(!reused_buffer_);
  reused_buffer_ = reused_buffer;
}

}  // namespace pax
