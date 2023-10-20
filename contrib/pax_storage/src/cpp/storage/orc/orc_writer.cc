#include "comm/cbdb_api.h"

#include "comm/guc.h"
#include "comm/log.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/orc.h"
#include "storage/orc/orc_defined.h"

namespace pax {

std::vector<orc::proto::Type_Kind> OrcWriter::BuildSchema(TupleDesc desc) {
  std::vector<orc::proto::Type_Kind> type_kinds;
  for (int i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
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

OrcWriter::OrcWriter(
    const MicroPartitionWriter::WriterOptions &orc_writer_options,
    const std::vector<orc::proto::Type_Kind> &column_types, File *file)
    : MicroPartitionWriter(orc_writer_options),
      column_types_(column_types),
      file_(file),
      total_rows_(0),
      current_offset_(0) {
  pax_columns_ =
      new PaxColumns(column_types_, writer_options_.encoding_opts);

  TupleDesc desc = orc_writer_options.desc;
  for (int i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    Assert((size_t)i < pax_columns_->GetColumns());
    auto column = (*pax_columns_)[i];

    Assert(column);
    size_t align_size;
    switch (attr->attalign) {
      case TYPALIGN_SHORT:
        align_size = ALIGNOF_SHORT;
        break;
      case TYPALIGN_INT:
        align_size = ALIGNOF_INT;
        break;
      case TYPALIGN_DOUBLE:
        align_size = ALIGNOF_DOUBLE;
        break;
      case TYPALIGN_CHAR:
        align_size = PAX_DATA_NO_ALIGN;
        break;
      default:
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }

    column->SetAlignSize(align_size);
  }

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

  auto natts = static_cast<int>(column_types.size());
  auto stats_data = new OrcColumnStatsData();
  stats_collector_.SetStatsMessage(stats_data->Initialize(natts), natts);
}

OrcWriter::~OrcWriter() {
  delete pax_columns_;
  delete file_;
}

MicroPartitionWriter *OrcWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  if (mpstats) {
    auto stats_data = new MicroPartittionFileStatsData(&summary_.mp_stats, static_cast<int>(column_types_.size()));
    mpstats->SetStatsMessage(stats_data, column_types_.size());
  }
  return MicroPartitionWriter::SetStatsCollector(mpstats);
}

void OrcWriter::Flush() {
  BufferedOutputStream buffer_mem_stream(2048);
  if (WriteStripe(&buffer_mem_stream)) {
    Assert(current_offset_ >= buffer_mem_stream.GetDataBuffer()->Used());
    file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                   buffer_mem_stream.GetDataBuffer()->Used(),
                   current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());
    file_->Flush();
    delete pax_columns_;
    pax_columns_ = new PaxColumns(column_types_, writer_options_.encoding_opts);
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
  stats_collector_.AddRow(table_slot);

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

  if (pax_columns_->GetRows() >= 16384) {
    Flush();
  }
}

void OrcWriter::WriteTupleN(CTupleSlot **slot, size_t n) {
  // TODO(jiaqizho): support WriteTupleN
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream) {
  std::vector<orc::proto::Stream> streams;
  std::vector<ColumnEncoding> encoding_kinds;
  orc::proto::StripeFooter stripe_footer;
  orc::proto::StripeInformation *stripe_info;
  orc::proto::StripeStatistics *stripe_stats;

  size_t data_len = 0;
  size_t number_of_row = pax_columns_->GetRows();

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
                        int64 origin_len) {
        ColumnEncoding column_encoding;
        Assert(encoding_kind !=
               ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
        if (encoding_kind != ColumnEncoding_Kind_NO_ENCODED &&
            origin_len == NO_ENCODE_ORIGIN_LEN) {
          CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
        }
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

  stripe_stats = meta_data_.add_stripestats();
  auto stats_data = dynamic_cast<OrcColumnStatsData*>(stats_collector_.GetStatsData());
  Assert(stats_data);
  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
    auto pb_stats = stripe_stats->add_colstats();
    PaxColumn *pax_column = (*pax_columns_)[i];

    *stripe_footer.add_pax_col_encodings() = encoding_kinds[i];

    pb_stats->set_hasnull(pax_column->HasNull());
    pb_stats->set_allnull(pax_column->AllNull());
    pb_stats->set_numberofvalues(pax_column->GetRows());
    *pb_stats->mutable_coldatastats() = *stats_data->GetColumnDataStats(static_cast<int>(i));
    PAX_LOG_IF(pax_enable_debug, "write group[%lu](allnull=%s, hasnull=%s, nrows=%lu)",
      i,
      pax_column->AllNull() ? "true" : "false",
      pax_column->HasNull() ? "true" : "false",
      pax_column->GetRows());
  }
  stats_data->Reset();
  stats_collector_.LightReset();

  stripe_footer.set_writertimezone("GMT");
  buffer_mem_stream->Set(data_buffer);

  // check memory io with protobuf
  CBDB_CHECK(stripe_footer.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  stripe_info = file_footer_.add_stripes();

  stripe_info->set_offset(current_offset_);
  stripe_info->set_indexlength(0);
  stripe_info->set_datalength(data_len);
  stripe_info->set_footerlength(buffer_mem_stream->GetSize());
  stripe_info->set_numberofrows(number_of_row);

  current_offset_ += buffer_mem_stream->GetSize();
  total_rows_ += number_of_row;

  return true;
}

void OrcWriter::Close() {
  BufferedOutputStream buffer_mem_stream(2048);
  size_t file_offset = current_offset_;
  bool empty_stripe = false;
  DataBuffer<char> *data_buffer;

  empty_stripe = !WriteStripe(&buffer_mem_stream);
  if (empty_stripe) {
    data_buffer = new DataBuffer<char>(2048);
    buffer_mem_stream.Set(data_buffer);
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
  if (empty_stripe) {
    delete data_buffer;
  }
}

size_t OrcWriter::PhysicalSize() const { return pax_columns_->PhysicalSize(); }

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
  auto ok = meta_data_.SerializeToZeroCopyStream(buffer_mem_stream);
  PAX_LOG_IF(!ok, "%s SerializeToZeroCopyStream failed:%m", __func__);
  CBDB_CHECK(ok,
             cbdb::CException::ExType::kExTypeIOError);

  post_script_.set_metadatalength(buffer_mem_stream->EndBufferOutRecord());
}

void OrcWriter::WriteFileFooter(BufferedOutputStream *buffer_mem_stream) {
  file_footer_.set_contentlength(current_offset_ - file_footer_.headerlength());
  file_footer_.set_numberofrows(total_rows_);

  auto stats_data = dynamic_cast<OrcColumnStatsData*>(stats_collector_.GetStatsData());
  Assert(file_footer_.colinfo_size() == 0);
  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
    auto pb_stats = file_footer_.add_statistics();
    // FIXME(jiaqizho): the statistics in file footer is not accurate
    // but statistics in stripe stats is accurate
    pb_stats->set_hasnull(false);
    pb_stats->set_numberofvalues((*pax_columns_)[i]->GetRows());

    auto pb_colinfo = file_footer_.add_colinfo();
    *pb_colinfo = *stats_data->GetColumnBasicInfo(static_cast<int>(i));
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

  auto ps_len = (uint64)buffer_mem_stream->EndBufferOutRecord();
  Assert(ps_len > 0);
  static_assert(sizeof(ps_len) == ORC_POST_SCRIPT_SIZE,
                "post script type len not match.");
  buffer_mem_stream->DirectWrite((char *)&ps_len, ORC_POST_SCRIPT_SIZE);
}

OrcColumnStatsData *OrcColumnStatsData::Initialize(int natts) {
  Assert(natts > 0);
  col_data_stats_.resize(natts);
  col_basic_info_.resize(natts);
  Reset();
  return this;
}
void OrcColumnStatsData::CheckVectorSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
}

void OrcColumnStatsData::Reset() {
  auto n = col_basic_info_.size();
  for (size_t i = 0; i < n; i++) {
    col_data_stats_[i].Clear();
  }
}

::pax::stats::ColumnBasicInfo *OrcColumnStatsData::GetColumnBasicInfo(int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_basic_info_[column_index];
}

::pax::stats::ColumnDataStats *OrcColumnStatsData::GetColumnDataStats(int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_data_stats_[column_index];
}

int OrcColumnStatsData::ColumnSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
  return static_cast<int>(col_basic_info_.size());
}

// PaxColumns has updated all null
void OrcColumnStatsData::SetAllNull(int column_index, bool allnull) {
}

// PaxColumns has updated has null
void OrcColumnStatsData::SetHasNull(int column_index, bool hasnull) {
}
}  // namespace pax
