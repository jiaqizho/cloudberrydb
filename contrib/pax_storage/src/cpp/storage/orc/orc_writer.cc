#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_group.h"
#include "storage/orc/porc.h"
#include "storage/pax_itemptr.h"
#include "storage/toast/pax_toast.h"

namespace pax {

std::vector<pax::porc::proto::Type_Kind> OrcWriter::BuildSchema(TupleDesc desc,
                                                                bool is_vec) {
  std::vector<pax::porc::proto::Type_Kind> type_kinds;
  for (int i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    if (attr->attbyval) {
      switch (attr->attlen) {
        case 1:
          if (attr->atttypid == BOOLOID) {
            type_kinds.emplace_back(
                pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN);
          } else {
            type_kinds.emplace_back(
                pax::porc::proto::Type_Kind::Type_Kind_BYTE);
          }
          break;
        case 2:
          type_kinds.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_SHORT);
          break;
        case 4:
          type_kinds.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_INT);
          break;
        case 8:
          type_kinds.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_LONG);
          break;
        default:
          Assert(!"should not be here! pg_type which attbyval=true only have typlen of "
                  "1, 2, 4, or 8");
      }
    } else {
      Assert(attr->attlen > 0 || attr->attlen == -1);
      if (attr->atttypid == NUMERICOID) {
        type_kinds.emplace_back(
            is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL
                   : pax::porc::proto::Type_Kind::Type_Kind_DECIMAL);
      } else if (attr->atttypid == BPCHAROID) {
        type_kinds.emplace_back(
            is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR
                   : pax::porc::proto::Type_Kind::Type_Kind_BPCHAR);
      } else {
        type_kinds.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
      }
    }
  }

  return type_kinds;
}

static PaxColumn *CreateDecimalColumn(bool is_vec,
                                      const PaxEncoder::EncodingOption &opts) {
  if (is_vec) {
    return (PaxColumn *)
        traits::ColumnOptCreateTraits2<PaxShortNumericColumn>::create_encoding(
            DEFAULT_CAPACITY, opts);
  } else {
    return (PaxColumn *)
        traits::ColumnOptCreateTraits2<PaxPgNumericColumn>::create_encoding(
            DEFAULT_CAPACITY, DEFAULT_CAPACITY, opts);
  }
}

static PaxColumn *CreateBpCharColumn(bool is_vec,
                                     const PaxEncoder::EncodingOption &opts) {
  return is_vec
             ? (PaxColumn *)traits::ColumnOptCreateTraits2<
                   PaxVecBpCharColumn>::create_encoding(DEFAULT_CAPACITY,
                                                        DEFAULT_CAPACITY, opts)
             : (PaxColumn *)traits::ColumnOptCreateTraits2<
                   PaxBpCharColumn>::create_encoding(DEFAULT_CAPACITY,
                                                     DEFAULT_CAPACITY, opts);
}

static PaxColumn *CreateBitPackedColumn(
    bool is_vec, const PaxEncoder::EncodingOption &opts) {
  return is_vec
             ? (PaxColumn *)traits::ColumnOptCreateTraits2<
                   PaxVecBitPackedColumn>::create_encoding(DEFAULT_CAPACITY,
                                                           opts)
             : (PaxColumn *)traits::ColumnOptCreateTraits2<
                   PaxBitPackedColumn>::create_encoding(DEFAULT_CAPACITY, opts);
}

template <typename N>
static PaxColumn *CreateCommColumn(bool is_vec,
                                   const PaxEncoder::EncodingOption &opts) {
  return is_vec
             ? (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxVecEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                             opts)
             : (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                          opts);
}

static PaxColumns *BuildColumns(
    const std::vector<pax::porc::proto::Type_Kind> &types,
    const std::vector<std::tuple<ColumnEncoding_Kind, int>>
        &column_encoding_types,
    const std::pair<ColumnEncoding_Kind, int> &lengths_encoding_types,
    const PaxStorageFormat &storage_format) {
  PaxColumns *columns;
  bool is_vec;

  columns = PAX_NEW<PaxColumns>();
  is_vec = (storage_format == PaxStorageFormat::kTypeStoragePorcVec);
  columns->SetStorageFormat(storage_format);

  for (size_t i = 0; i < types.size(); i++) {
    auto type = types[i];

    PaxEncoder::EncodingOption encoding_option;
    encoding_option.column_encode_type = std::get<0>(column_encoding_types[i]);
    encoding_option.is_sign = true;
    encoding_option.compress_level = std::get<1>(column_encoding_types[i]);

    if (lengths_encoding_types.first == ColumnEncoding_Kind_DEF_ENCODED) {
      // default value of lengths_stream is zstd
      encoding_option.lengths_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
      encoding_option.lengths_compress_level = 5;
    } else {
      encoding_option.lengths_encode_type = lengths_encoding_types.first;
      encoding_option.lengths_compress_level = lengths_encoding_types.second;
    }

    switch (type) {
      case (pax::porc::proto::Type_Kind::Type_Kind_STRING): {
        encoding_option.is_sign = false;
        columns->Append(
            is_vec ? (PaxColumn *)traits::ColumnOptCreateTraits2<
                         PaxVecNonFixedEncodingColumn>::
                         create_encoding(DEFAULT_CAPACITY, DEFAULT_CAPACITY,
                                         std::move(encoding_option))
                   : (PaxColumn *)traits::ColumnOptCreateTraits2<
                         PaxNonFixedEncodingColumn>::
                         create_encoding(DEFAULT_CAPACITY, DEFAULT_CAPACITY,
                                         std::move(encoding_option)));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR):
      case (pax::porc::proto::Type_Kind::Type_Kind_BPCHAR): {
        columns->Append(CreateBpCharColumn(is_vec, std::move(encoding_option)));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL):
      case (pax::porc::proto::Type_Kind::Type_Kind_DECIMAL): {
        AssertImply(is_vec,
                    type == pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL);
        AssertImply(!is_vec,
                    type == pax::porc::proto::Type_Kind::Type_Kind_DECIMAL);
        columns->Append(
            CreateDecimalColumn(is_vec, std::move(encoding_option)));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN):
        columns->Append(
            CreateBitPackedColumn(is_vec, std::move(encoding_option)));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_BYTE):  // len 1 integer
        columns->Append(
            CreateCommColumn<int8>(is_vec, std::move(encoding_option)));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_SHORT):  // len 2 integer
        columns->Append(
            CreateCommColumn<int16>(is_vec, std::move(encoding_option)));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_INT):  // len 4 integer
        columns->Append(
            CreateCommColumn<int32>(is_vec, std::move(encoding_option)));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_LONG):  // len 8 integer
        columns->Append(
            CreateCommColumn<int64>(is_vec, std::move(encoding_option)));
        break;
      default:
        Assert(!"non-implemented column type");
        break;
    }
  }

  return columns;
}

OrcWriter::OrcWriter(
    const MicroPartitionWriter::WriterOptions &writer_options,
    const std::vector<pax::porc::proto::Type_Kind> &column_types, File *file,
    File *toast_file)
    : MicroPartitionWriter(writer_options),
      is_closed_(false),
      column_types_(column_types),
      file_(file),
      toast_file_(toast_file),
      current_written_phy_size_(0),
      row_index_(0),
      total_rows_(0),
      current_offset_(0),
      current_toast_file_offset_(0) {
  pax_columns_ = BuildColumns(column_types_, writer_options.encoding_opts,
                              writer_options.lengths_encoding_opts,
                              writer_options.storage_format);

  TupleDesc desc = writer_options.rel_tuple_desc;
  Assert(desc);
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

    // init the datum holder
    origin_datum_holder_.emplace_back(0);
    detoast_holder_.emplace_back(0);
    gen_toast_holder_.emplace_back(0);
  }

  summary_.rel_oid = writer_options.rel_oid;
  summary_.block_id = writer_options.block_id;
  summary_.file_name = writer_options.file_name;

  post_script_.set_footerlength(0);
  post_script_.set_majorversion(PAX_MAJOR_VERSION);
  post_script_.set_minorversion(PAX_MINOR_VERSION);
  post_script_.set_writer(PORC_WRITER_ID);
  post_script_.set_magic(PORC_MAGIC_ID);

  auto natts = static_cast<int>(column_types.size());
  auto stats_data = PAX_NEW<OrcColumnStatsData>();
  stats_collector_.SetStatsMessage(stats_data->Initialize(natts), natts);
}

OrcWriter::~OrcWriter() {
  PAX_DELETE(pax_columns_);
  PAX_DELETE(file_);
  PAX_DELETE(toast_file_);
}

MicroPartitionWriter *OrcWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  if (mpstats) {
    auto stats_data = PAX_NEW<MicroPartittionFileStatsData>(
        &summary_.mp_stats, static_cast<int>(column_types_.size()));
    mpstats->SetStatsMessage(stats_data, column_types_.size());
    stats_collector_.SetMinMaxColumnIndex(
        std::vector<int>(mpstats->GetMinMaxColumnIndex()));

    return MicroPartitionWriter::SetStatsCollector(mpstats);
  }
  return MicroPartitionWriter::SetStatsCollector(mpstats);
}

void OrcWriter::Flush() {
  BufferedOutputStream buffer_mem_stream(2048);
  DataBuffer<char> toast_mem(nullptr, 0, true, false);
  if (WriteStripe(&buffer_mem_stream, &toast_mem)) {
    current_written_phy_size_ += pax_columns_->PhysicalSize();
    Assert(current_offset_ >= buffer_mem_stream.GetDataBuffer()->Used());
    summary_.file_size += buffer_mem_stream.GetDataBuffer()->Used();
    file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                   buffer_mem_stream.GetDataBuffer()->Used(),
                   current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());
    if (toast_mem.GetBuffer()) {
      Assert(toast_file_);
      Assert(current_toast_file_offset_ >= toast_mem.Used());
      toast_file_->PWriteN(toast_mem.GetBuffer(), toast_mem.Used(),
                           current_toast_file_offset_ - toast_mem.Used());
    }
    PAX_DELETE(pax_columns_);
    pax_columns_ = BuildColumns(column_types_, writer_options_.encoding_opts,
                                writer_options_.lengths_encoding_opts,
                                writer_options_.storage_format);
  }
}

void OrcWriter::PerpareWriteTuple(TupleTableSlot *table_slot) {
  TupleDesc tuple_desc;
  int16 type_len;
  bool type_by_val;
  bool is_null;
  Datum tts_value;
  char type_storage;
  struct varlena *tts_value_vl = nullptr, *detoast_vl = nullptr;

  tuple_desc = writer_options_.rel_tuple_desc;
  Assert(tuple_desc);
  for (int i = 0; i < tuple_desc->natts; i++) {
    auto attrs = TupleDescAttr(tuple_desc, i);
    type_len = attrs->attlen;
    type_by_val = attrs->attbyval;
    is_null = table_slot->tts_isnull[i];
    tts_value = table_slot->tts_values[i];
    type_storage = attrs->attstorage;

    AssertImply(attrs->attisdropped, is_null);

    if (is_null) {
      continue;
    }

    // prepare toast
    if (!type_by_val && type_len == -1) {
      tts_value_vl = (struct varlena *)DatumGetPointer(tts_value);
      origin_datum_holder_[i] = tts_value;

      // Once passin toast is compress toast and datum is within the range
      // allowed by PAX, then PAX will direct store it
      if (VARATT_IS_COMPRESSED(tts_value_vl)) {
        auto compress_toast_extsize =
            VARDATA_COMPRESSED_GET_EXTSIZE(tts_value_vl);

        if (type_storage != TYPSTORAGE_PLAIN &&
            !VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST_BY_SIZE(
                compress_toast_extsize) &&
            VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST_BY_SIZE(
                compress_toast_extsize)) {
          continue;
        }
      }

      // still detoast the origin toast
      detoast_vl = cbdb::PgDeToastDatum(tts_value_vl);
      Assert(detoast_vl != nullptr);
      detoast_holder_[i] = PointerGetDatum(detoast_vl);

      if (tts_value_vl != detoast_vl) {
        table_slot->tts_values[i] = PointerGetDatum(detoast_vl);
      }

      // only make toast here
      Datum new_toast =
          pax_make_toast(PointerGetDatum(detoast_vl), type_storage);

      if (new_toast) {
        gen_toast_holder_[i] = new_toast;
        table_slot->tts_values[i] = new_toast;
      }
    }
  }
}

void OrcWriter::WriteTuple(TupleTableSlot *table_slot) {
  int natts;
  TupleDesc tuple_desc;
  int16 type_len;
  bool type_by_val;
  bool is_null;
  Datum tts_value;
  struct varlena *tts_value_vl = nullptr;
  PerpareWriteTuple(table_slot);

  // The reason why
  tuple_desc = writer_options_.rel_tuple_desc;
  Assert(tuple_desc);

  SetTupleOffset(&table_slot->tts_tid, row_index_++);
  natts = tuple_desc->natts;

  CBDB_CHECK(pax_columns_->GetColumns() == static_cast<size_t>(natts),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);

  for (int i = 0; i < natts; i++) {
    type_len = tuple_desc->attrs[i].attlen;
    type_by_val = tuple_desc->attrs[i].attbyval;
    is_null = table_slot->tts_isnull[i];
    tts_value = table_slot->tts_values[i];

    AssertImply(tuple_desc->attrs[i].attisdropped, is_null);

    if (is_null) {
      (*pax_columns_)[i]->AppendNull();
      continue;
    }

    if (type_by_val) {
      switch (type_len) {
        case 1: {
          auto value = cbdb::DatumToInt8(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 2: {
          auto value = cbdb::DatumToInt16(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 4: {
          auto value = cbdb::DatumToInt32(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 8: {
          auto value = cbdb::DatumToInt64(tts_value);
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
          tts_value_vl = (struct varlena *)DatumGetPointer(tts_value);
          if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns_)) {
            // NUMERIC requires a complete Datum
            // It won't get a toast
            if (tuple_desc->attrs[i].atttypid == NUMERICOID) {
              Assert((*pax_columns_)[i]->GetPaxColumnTypeInMem() ==
                     PaxColumnTypeInMem::kTypeVecDecimal);
              Assert(!VARATT_IS_PAX_SUPPORT_TOAST(tts_value_vl));
              (*pax_columns_)[i]->Append(reinterpret_cast<char *>(tts_value_vl),
                                         VARSIZE_ANY(tts_value_vl));

            } else {
              if (VARATT_IS_PAX_SUPPORT_TOAST(tts_value_vl)) {
                (*pax_columns_)[i]->AppendToast(
                    reinterpret_cast<char *>(tts_value_vl),
                    PAX_VARSIZE_ANY(tts_value_vl));
              } else {
                (*pax_columns_)[i]->Append(VARDATA_ANY(tts_value_vl),
                                           VARSIZE_ANY_EXHDR(tts_value_vl));
              }
            }
          } else {
            if (VARATT_IS_PAX_SUPPORT_TOAST(tts_value_vl)) {
              (*pax_columns_)[i]->AppendToast(
                  reinterpret_cast<char *>(tts_value_vl),
                  PAX_VARSIZE_ANY(tts_value_vl));
            } else {
              (*pax_columns_)[i]->Append(reinterpret_cast<char *>(tts_value_vl),
                                         VARSIZE_ANY(tts_value_vl));
            }
          }

          break;
        }
        default:
          Assert(type_len > 0);
          (*pax_columns_)[i]->Append(
              static_cast<char *>(cbdb::DatumToPointer(tts_value)), type_len);
          break;
      }
    }
  }

  summary_.num_tuples++;
  pax_columns_->AddRows(1);
  stats_collector_.AddRow(table_slot, tuple_desc, detoast_holder_);

  EndWriteTuple(table_slot);
}

void OrcWriter::EndWriteTuple(TupleTableSlot *table_slot) {
  TupleDesc tuple_desc;

  tuple_desc = writer_options_.rel_tuple_desc;
  Assert(tuple_desc);

  for (int i = 0; i < tuple_desc->natts; i++) {
    auto attrs = TupleDescAttr(tuple_desc, i);
    auto type_len = attrs->attlen;
    auto type_byval = attrs->attbyval;
    auto is_null = table_slot->tts_isnull[i];

    if (is_null || type_byval || type_len != -1) {
      continue;
    }

    // no need take care current datum refer
    table_slot->tts_values[i] = origin_datum_holder_[i];

    // detoast happend
    if (detoast_holder_[i] != origin_datum_holder_[i]) {
      cbdb::Pfree(DatumGetPointer(detoast_holder_[i]));
    }

    // created new pax toast
    if (gen_toast_holder_[i] != 0) {
      pax_free_toast(gen_toast_holder_[i]);
    }
    detoast_holder_[i] = 0;
    origin_datum_holder_[i] = 0;
    gen_toast_holder_[i] = 0;
  }

  if (pax_columns_->GetRows() >= writer_options_.group_limit) {
    Flush();
  }
}

void OrcWriter::MergeTo(MicroPartitionWriter *writer) {
  auto orc_writer = dynamic_cast<OrcWriter *>(writer);
  Assert(orc_writer);
  Assert(!is_closed_ && !(orc_writer->is_closed_));
  Assert(this != writer);
  Assert(writer_options_.rel_oid == orc_writer->writer_options_.rel_oid);

  // merge the groups which in disk
  MergeGroups(orc_writer);

  // clear the unstate file in disk.
  orc_writer->DeleteUnstateFile();

  // merge the memory
  MergePaxColumns(orc_writer);

  // Update summary
  summary_.num_tuples += orc_writer->summary_.num_tuples;
  if (mp_stats_) {
    mp_stats_->MergeTo(orc_writer->mp_stats_, writer_options_.rel_tuple_desc);
  }
}

void OrcWriter::MergePaxColumns(OrcWriter *writer) {
  PaxColumns *columns = writer->pax_columns_;
  bool ok pg_attribute_unused();
  Assert(columns->GetColumns() == pax_columns_->GetColumns());
  Assert(columns->GetRows() < writer_options_.group_limit);
  if (columns->GetRows() == 0) {
    return;
  }

  BufferedOutputStream buffer_mem_stream(2048);
  DataBuffer<char> toast_mem(nullptr, 0, true, false);
  ok = WriteStripe(&buffer_mem_stream, &toast_mem, columns,
                   &(writer->stats_collector_), writer->mp_stats_);

  // must be ok
  Assert(ok);

  file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                 buffer_mem_stream.GetDataBuffer()->Used(),
                 current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());

  // direct write the toast
  if (toast_mem.GetBuffer()) {
    Assert(toast_file_);
    Assert(current_toast_file_offset_ >= toast_mem.Used());
    toast_file_->PWriteN(toast_mem.GetBuffer(), toast_mem.Used(),
                         current_toast_file_offset_ - toast_mem.Used());
  }

  // Not do memory merge
}

void OrcWriter::MergeGroups(OrcWriter *orc_writer) {
  DataBuffer<char> merge_buffer(0);

  for (int index = 0; index < orc_writer->file_footer_.stripes_size();
       index++) {
    MergeGroup(orc_writer, index, &merge_buffer);
  }
}

void OrcWriter::MergeGroup(OrcWriter *orc_writer, int group_index,
                           DataBuffer<char> *merge_buffer) {
  const auto &stripe_info = orc_writer->file_footer_.stripes(group_index);
  auto total_len = stripe_info.footerlength();
  auto stripe_data_len = stripe_info.datalength();
  auto number_of_rows = stripe_info.numberofrows();
  auto number_of_toast = stripe_info.numberoftoast();
  auto toast_off = stripe_info.toastoffset();
  auto toast_len = stripe_info.toastlength();

  // will not flush empty group in disk
  Assert(stripe_data_len);

  if (!merge_buffer->GetBuffer()) {
    merge_buffer->Set(BlockBuffer::Alloc<char *>(total_len), total_len);
    merge_buffer->SetMemTakeOver(true);
  } else if (merge_buffer->Capacity() < total_len) {
    merge_buffer->Clear();
    merge_buffer->Set(BlockBuffer::Alloc<char *>(total_len), total_len);
  }
  orc_writer->file_->Flush();
  orc_writer->file_->PReadN(merge_buffer->GetBuffer(), total_len,
                            stripe_info.offset());

  summary_.file_size += total_len;
  file_->PWriteN(merge_buffer->GetBuffer(), total_len, current_offset_);

  // merge the toast file content
  // We could merge a single toast file directly into another file,
  // but this would make assumptions about the toast file

  // check the external toast exist
  if (toast_len > 0) {
    // must exist
    Assert(toast_file_);
    Assert(merge_buffer->GetBuffer());
    if (merge_buffer->Capacity() < toast_len) {
      merge_buffer->Clear();
      merge_buffer->Set(BlockBuffer::Alloc<char *>(toast_len), toast_len);
    }
    orc_writer->toast_file_->Flush();
    orc_writer->toast_file_->PReadN(merge_buffer->GetBuffer(), toast_len,
                                    toast_off);
    toast_file_->PWriteN(merge_buffer->GetBuffer(), toast_len,
                         current_toast_file_offset_);
  }

  auto stripe_info_write = file_footer_.add_stripes();

  stripe_info_write->set_offset(current_offset_);
  stripe_info_write->set_datalength(stripe_data_len);
  stripe_info_write->set_footerlength(total_len);
  stripe_info_write->set_numberofrows(number_of_rows);

  stripe_info_write->set_numberoftoast(number_of_toast);
  stripe_info_write->set_toastoffset(current_toast_file_offset_);
  stripe_info_write->set_toastlength(toast_len);

  current_toast_file_offset_ += toast_len;
  current_offset_ += total_len;
  total_rows_ += number_of_rows;

  Assert((size_t)stripe_info.colstats_size() == pax_columns_->GetColumns());

  for (int stats_index = 0; stats_index < stripe_info.colstats_size();
       stats_index++) {
    auto col_stats = stripe_info.colstats(stats_index);
    auto col_stats_write = stripe_info_write->add_colstats();
    col_stats_write->CopyFrom(col_stats);

    stripe_info_write->add_exttoastlength(
        stripe_info.exttoastlength(stats_index));
  }
}

void OrcWriter::DeleteUnstateFile() {
  auto fs = Singleton<LocalFileSystem>::GetInstance();
  auto file_path = file_->GetPath();
  file_->Close();
  fs->Delete(file_path);

  if (toast_file_) {
    auto toast_file_path = toast_file_->GetPath();
    toast_file_->Close();
    fs->Delete(toast_file_path);
  }

  is_closed_ = true;
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream,
                            DataBuffer<char> *toast_mem) {
  return WriteStripe(buffer_mem_stream, toast_mem, pax_columns_,
                     &stats_collector_, mp_stats_);
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream,
                            DataBuffer<char> *toast_mem,
                            PaxColumns *pax_columns,
                            MicroPartitionStats *stripe_stats,
                            MicroPartitionStats *file_stats) {
  std::vector<pax::porc::proto::Stream> streams;
  std::vector<ColumnEncoding> encoding_kinds;
  pax::porc::proto::StripeFooter stripe_footer;
  pax::porc::proto::StripeInformation *stripe_info;

  size_t data_len = 0;
  size_t number_of_row = pax_columns->GetRows();
  size_t toast_len = 0;
  size_t number_of_toast = pax_columns->ToastCounts();

  // No need add stripe if nothing in memeory
  if (number_of_row == 0) {
    return false;
  }

  PaxColumns::ColumnStreamsFunc column_streams_func =
      [&streams](const pax::porc::proto::Stream_Kind &kind, size_t column,
                 size_t length, size_t padding) {
        Assert(padding < MEMORY_ALIGN_SIZE);
        pax::porc::proto::Stream stream;
        stream.set_kind(kind);
        stream.set_column(static_cast<uint32>(column));
        stream.set_length(length);
        stream.set_padding(padding);

        streams.push_back(std::move(stream));
      };

  PaxColumns::ColumnEncodingFunc column_encoding_func =
      [&encoding_kinds](const ColumnEncoding_Kind &encoding_kind,
                        const uint64 compress_lvl, const int64 origin_len,
                        const ColumnEncoding_Kind &length_stream_encoding_kind,
                        const uint64 length_stream_compress_lvl,
                        const int64 length_stream_origin_len) {
        ColumnEncoding column_encoding;
        Assert(encoding_kind !=
               ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
        column_encoding.set_kind(encoding_kind);
        column_encoding.set_compress_lvl(compress_lvl);
        column_encoding.set_length(origin_len);

        column_encoding.set_length_stream_kind(length_stream_encoding_kind);
        column_encoding.set_length_stream_compress_lvl(
            length_stream_compress_lvl);
        column_encoding.set_length_stream_length(length_stream_origin_len);

        encoding_kinds.push_back(std::move(column_encoding));
      };

  DataBuffer<char> *data_buffer =
      pax_columns->GetDataBuffer(column_streams_func, column_encoding_func);

  Assert(data_buffer->Used() == data_buffer->Capacity());

  for (const auto &stream : streams) {
    *stripe_footer.add_streams() = stream;
    data_len += stream.length();
  }

  stripe_info = file_footer_.add_stripes();
  auto stats_data =
      dynamic_cast<OrcColumnStatsData *>(stripe_stats->GetStatsData());
  Assert(stats_data);
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    auto pb_stats = stripe_info->add_colstats();
    PaxColumn *pax_column = (*pax_columns)[i];

    *stripe_footer.add_pax_col_encodings() = encoding_kinds[i];

    pb_stats->set_hastoast(pax_column->ToastCounts() > 0);
    pb_stats->set_hasnull(pax_column->HasNull());
    pb_stats->set_allnull(pax_column->AllNull());
    *pb_stats->mutable_coldatastats() =
        *stats_data->GetColumnDataStats(static_cast<int>(i));
    PAX_LOG_IF(
        pax_enable_debug,
        "write group[%lu](allnull=%s, hasnull=%s, hastoast=%s, nrows=%lu)", i,
        pax_column->AllNull() ? "true" : "false",
        pax_column->HasNull() ? "true" : "false",
        pax_column->ToastCounts() > 0 ? "true" : "false",
        pax_column->GetRows());
  }
  if (file_stats) {
    file_stats->MergeTo(stripe_stats, writer_options_.rel_tuple_desc);
  }

  stats_data->Reset();
  stripe_stats->LightReset();

  buffer_mem_stream->Set(data_buffer);

  // check memory io with protobuf
  CBDB_CHECK(stripe_footer.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  // Begin deal the toast memory
  if (number_of_toast > 0) {
    auto external_data_buffer = pax_columns->GetExternalToastDataBuffer();
    toast_len = external_data_buffer->Used();
    if (toast_len > 0) {
      Assert(!toast_mem->GetBuffer());
      toast_mem->Set(external_data_buffer->GetBuffer(), toast_len);
      toast_mem->BrushAll();
    }
  }

  stripe_info->set_offset(current_offset_);
  stripe_info->set_datalength(data_len);
  stripe_info->set_footerlength(buffer_mem_stream->GetSize());
  stripe_info->set_numberofrows(number_of_row);
  stripe_info->set_toastoffset(current_toast_file_offset_);
  stripe_info->set_toastlength(toast_len);
  stripe_info->set_numberoftoast(number_of_toast);
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    PaxColumn *pax_column = (*pax_columns)[i];
    auto ext_buffer = pax_column->GetExternalToastDataBuffer();
    stripe_info->add_exttoastlength(ext_buffer ? ext_buffer->Used() : 0);
  }

  current_offset_ += buffer_mem_stream->GetSize();
  current_toast_file_offset_ += toast_len;
  total_rows_ += number_of_row;

  return true;
}

void OrcWriter::Close() {
  if (is_closed_) {
    return;
  }
  BufferedOutputStream buffer_mem_stream(2048);
  size_t file_offset = current_offset_;
  bool empty_stripe = false;
  DataBuffer<char> *data_buffer;
  DataBuffer<char> toast_mem(nullptr, 0, true, false);

  empty_stripe = !WriteStripe(&buffer_mem_stream, &toast_mem);
  if (empty_stripe) {
    data_buffer = PAX_NEW<DataBuffer<char>>(2048);
    buffer_mem_stream.Set(data_buffer);
  }

  WriteFileFooter(&buffer_mem_stream);
  WritePostscript(&buffer_mem_stream);
  if (summary_callback_) {
    summary_.file_size += buffer_mem_stream.GetDataBuffer()->Used();
    summary_callback_(summary_);
  }

  file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                 buffer_mem_stream.GetDataBuffer()->Used(), file_offset);

  if (toast_mem.GetBuffer()) {
    Assert(toast_file_);
    Assert(current_toast_file_offset_ >= toast_mem.Used());
    toast_file_->PWriteN(toast_mem.GetBuffer(), toast_mem.Used(),
                         current_toast_file_offset_ - toast_mem.Used());
  }

  // Close toast_file before origin file
  if (toast_file_) {
    auto toast_file_path = toast_file_->GetPath();
    toast_file_->Flush();
    toast_file_->Close();
    // no toast happend
    if (current_toast_file_offset_ == 0) {
      Singleton<LocalFileSystem>::GetInstance()->Delete(toast_file_path);
    }
  }

  file_->Flush();
  file_->Close();
  if (empty_stripe) {
    PAX_DELETE(data_buffer);
  }
  is_closed_ = true;
}

size_t OrcWriter::PhysicalSize() const {
  return current_written_phy_size_ + pax_columns_->PhysicalSize();
}

void OrcWriter::WriteFileFooter(BufferedOutputStream *buffer_mem_stream) {
  Assert(writer_options_.storage_format == kTypeStoragePorcNonVec ||
         writer_options_.storage_format == kTypeStoragePorcVec);
  file_footer_.set_contentlength(current_offset_);
  file_footer_.set_numberofrows(total_rows_);
  file_footer_.set_storageformat(writer_options_.storage_format);

  // build types and column attributes
  auto proto_type = file_footer_.add_types();
  proto_type->set_kind(::pax::porc::proto::Type_Kind_STRUCT);

  for (size_t i = 0; i < column_types_.size(); ++i) {
    auto orc_type = column_types_[i];

    auto sub_proto_type = file_footer_.add_types();
    sub_proto_type->set_kind(orc_type);
    auto pax_column = (*pax_columns_)[i];
    if (pax_column) {
      auto column_attrs = pax_column->GetAttributes();
      for (const auto &kv : column_attrs) {
        auto attr_pair = sub_proto_type->add_attributes();
        attr_pair->set_key(kv.first);
        attr_pair->set_value(kv.second);
      }
    }
    file_footer_.mutable_types(0)->add_subtypes(i);
  }

  auto stats_data =
      dynamic_cast<OrcColumnStatsData *>(stats_collector_.GetStatsData());
  Assert(file_footer_.colinfo_size() == 0);
  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
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
  static_assert(sizeof(ps_len) == PORC_POST_SCRIPT_SIZE,
                "post script type len not match.");
  buffer_mem_stream->DirectWrite((char *)&ps_len, PORC_POST_SCRIPT_SIZE);
}

OrcColumnStatsData *OrcColumnStatsData::Initialize(int natts) {
  Assert(natts >= 0);
  col_data_stats_.resize(natts);
  col_basic_info_.resize(natts);
  has_nulls_.resize(natts);
  all_nulls_.resize(natts);

  Reset();
  return this;
}

void OrcColumnStatsData::CopyFrom(MicroPartitionStatsData * /*stats*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}

void OrcColumnStatsData::CheckVectorSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
}

void OrcColumnStatsData::Reset() {
  auto n = col_basic_info_.size();
  for (size_t i = 0; i < n; i++) {
    col_data_stats_[i].Clear();
    has_nulls_[i] = false;
    all_nulls_[i] = true;
  }
}

::pax::stats::ColumnBasicInfo *OrcColumnStatsData::GetColumnBasicInfo(
    int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_basic_info_[column_index];
}

::pax::stats::ColumnDataStats *OrcColumnStatsData::GetColumnDataStats(
    int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_data_stats_[column_index];
}

int OrcColumnStatsData::ColumnSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
  return static_cast<int>(col_basic_info_.size());
}

void OrcColumnStatsData::SetAllNull(int column_index, bool allnull) {
  all_nulls_[column_index] = allnull;
}

void OrcColumnStatsData::SetHasNull(int column_index, bool hasnull) {
  has_nulls_[column_index] = hasnull;
}

bool OrcColumnStatsData::GetAllNull(int column_index) {
  return all_nulls_[column_index];
}

bool OrcColumnStatsData::GetHasNull(int column_index) {
  return has_nulls_[column_index];
}
}  // namespace pax
