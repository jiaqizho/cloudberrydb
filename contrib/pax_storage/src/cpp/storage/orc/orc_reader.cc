#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_cache.h"
#include "storage/columns/pax_column_int.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"
#include "storage/orc/orc.h"
#include "storage/pax_filter.h"

namespace pax {

OrcReader::OrcReader(File *file)
    : file_(file),
      reused_buffer_(nullptr),
      working_pax_columns_(nullptr),
      num_of_stripes_(0),
      proj_map_(nullptr),
      proj_len_(0),
      is_close_(true) {}

OrcReader::~OrcReader() { delete file_; }

PaxColumns *OrcReader::GetAllColumns() {
  Assert(GetNumberOfStripes() == 1);

  if (!working_pax_columns_) {
#ifdef ENABLE_PLASMA
    if (pax_column_cache_) {
      PaxColumns *columns_readed = nullptr;
      bool *proj_copy = nullptr;
      bool still_remain = false;
      std::tie(working_pax_columns_, release_key_, proj_copy) =
          pax_column_cache_->ReadCache();

      for (size_t i = 0; i < proj_len_; i++) {
        if (proj_copy[i]) {
          still_remain = true;
          break;
        }
      }

      if (still_remain) {
        columns_readed =
            ReadStripe(current_stripe_index_++, proj_copy, proj_len_);
        pax_column_cache_->WriteCache(columns_readed);
      } else {
        current_stripe_index_++;
      }

      delete[] proj_copy;

      // No get the cache columns
      if (working_pax_columns_->GetRows() == 0) {
        Assert(columns_readed);
        delete working_pax_columns_;
        working_pax_columns_ = columns_readed;
      } else if (still_remain) {
        Assert(columns_readed);
        working_pax_columns_->Merge(columns_readed);
      }

    } else {
      working_pax_columns_ =
          ReadStripe(current_stripe_index_++, proj_map_, proj_len_);
    }
#else  // ENABLE_PLASMA
    working_pax_columns_ =
        ReadStripe(current_stripe_index_++, proj_map_, proj_len_);

#endif  // ENABLE_PLASMA
    current_row_index_ = 0;
    for (size_t i = 0; i < column_types_.size(); i++) {
      current_nulls_[i] = 0;
#ifdef ENABLE_DEBUG
      auto column = (*working_pax_columns_)[i];
      if (column && !column->GetBuffer().first) {
        auto bm = column->GetBitmap();
        Assert(bm);
        for (size_t n = 0; n < column->GetRows(); n++) {
          Assert(!bm->Test(n));
        }
      }
#endif  // ENABLE_DEBUG
    }
  }

  return working_pax_columns_;
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

    Bitmap8 *non_null_bitmap = nullptr;
    bool has_null = stripe_info->stripe_statistics.colstats(index).hasnull();
    if (has_null) {
      const orc::proto::Stream &non_null_stream =
          stripe_footer.streams(streams_index++);
      auto bm_nbytes = static_cast<uint32>(non_null_stream.length());
      auto bm_bytes =
          reinterpret_cast<uint8 *>(data_buffer->GetAvailableBuffer());

      non_null_bitmap = new Bitmap8(BitmapRaw<uint8>(bm_bytes, bm_nbytes),
                                    BitmapTpl<uint8>::ReadOnlyRefBitmap);
      data_buffer->Brush(bm_nbytes);
    }

    switch (column_types_[index]) {
      case (orc::proto::Type_Kind::Type_Kind_STRING): {
        uint32 column_lens_size = 0;
        uint64 column_lens_len = 0;
        uint64 column_data_len = 0;
        DataBuffer<int64> *column_len_buffer = nullptr;
        DataBuffer<char> *column_data_buffer = nullptr;
        PaxNonFixedColumn *pax_column = nullptr;

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

#ifdef ENABLE_DEBUG
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
          pax_column = new PaxNonFixedColumn(0);
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
    Assert(pax_columns->GetColumns() > 0);
    auto last_column = (*pax_columns)[pax_columns->GetColumns() - 1];
    if (has_null) {
      Assert(non_null_bitmap);
      last_column->SetBitmap(non_null_bitmap);
    }
    last_column->SetRows(stripe_info->numbers_of_row);
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
  size_t file_length = 0;
  uint64 post_script_len = 0;

  Assert(file_);
  Assert(is_close_);

  // Must not open twice.
  Assert(!reused_buffer_);
  if (options.reused_buffer) {
    CBDB_CHECK(options.reused_buffer->IsMemTakeOver(),
               cbdb::CException::ExType::kExTypeLogicError);
    options.reused_buffer->BrushBackAll();

    reused_buffer_ = options.reused_buffer;
  }

  Assert(!proj_map_ && !proj_len_);
  if (options.filter)
    std::tie(proj_map_, proj_len_) = options.filter->GetColumnProjection();

#ifdef ENABLE_PLASMA
  if (options.pax_cache)
    pax_column_cache_ = new PaxColumnCache(options.pax_cache, options.block_id,
                                           proj_map_, proj_len_);
#endif

  auto read_in_disk = [this](size_t file_size, size_t post_script_len,
                             bool skip_read_post_script) {
    // read post script
    if (!skip_read_post_script) {
      char post_script_buffer[post_script_len];
      off_t offset;

      offset = (off_t)(file_size - ORC_POST_SCRIPT_SIZE - post_script_len);
      Assert(offset >= 0);

      file_->PReadN(post_script_buffer, post_script_len, offset);

      CBDB_CHECK(post_script_.ParseFromArray(&post_script_buffer,
                                             static_cast<int>(post_script_len)),
                 cbdb::CException::ExType::kExTypeIOError);
    }

    size_t footer_len = post_script_.footerlength();
    size_t tail_len = ORC_POST_SCRIPT_SIZE + post_script_len + footer_len;
    size_t footer_offset = file_size - tail_len;

    // read file_footer
    {
      char buffer[footer_len];

      file_->PReadN(&buffer, footer_len, footer_offset);

      SeekableInputStream input_stream(buffer, footer_len);
      CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
                 cbdb::CException::ExType::kExTypeIOError);
    }
  };

  file_length = file_->FileLength();
  if (file_length > ORC_TAIL_SIZE) {
    size_t footer_len;
    size_t tail_len;
    size_t footer_offset;
    char tail_buffer[ORC_TAIL_SIZE];

    file_->PReadN(tail_buffer, ORC_TAIL_SIZE,
                  (off_t)(file_length - ORC_TAIL_SIZE));

    static_assert(sizeof(post_script_len) == ORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    memcpy(&post_script_len, &tail_buffer[ORC_TAIL_SIZE - ORC_POST_SCRIPT_SIZE],
           ORC_POST_SCRIPT_SIZE);
    if (post_script_len + ORC_POST_SCRIPT_SIZE > ORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, false);
      goto finish_read;
    }

    auto post_script_offset =
        (off_t)(ORC_TAIL_SIZE - ORC_POST_SCRIPT_SIZE - post_script_len);
    CBDB_CHECK(post_script_.ParseFromArray(tail_buffer + post_script_offset,
                                           static_cast<int>(post_script_len)),
               cbdb::CException::ExType::kExTypeIOError);

    footer_len = post_script_.footerlength();
    tail_len = ORC_POST_SCRIPT_SIZE + post_script_len + footer_len;
    if (tail_len > ORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, true);
      goto finish_read;
    }

    footer_offset = ORC_TAIL_SIZE - tail_len;
    SeekableInputStream input_stream(tail_buffer + footer_offset, footer_len);
    CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
               cbdb::CException::ExType::kExTypeIOError);
  } else {
    static_assert(sizeof(post_script_len) == ORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    file_->PReadN(&post_script_len, ORC_POST_SCRIPT_SIZE,
                  (off_t)(file_length - ORC_POST_SCRIPT_SIZE));
    read_in_disk(file_length, post_script_len, false);
  }

finish_read:
  BuildProtoTypes();
  current_nulls_ = new uint32[column_types_.size()];
  memset(current_nulls_, 0, column_types_.size() * sizeof(uint32));

  num_of_stripes_ = file_footer_.stripes_size();

  // read meta parts
  if (post_script_.metadatalength() != 0) {
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

  is_close_ = false;
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
  if (is_close_) {
    return;
  }

#ifdef ENABLE_PLASMA
  if (pax_column_cache_) {
    pax_column_cache_->ReleaseCache(release_key_);
    delete pax_column_cache_;
  }
#endif

  ResetCurrentReading();
  delete[] current_nulls_;
  current_nulls_ = nullptr;
  file_->Close();
  is_close_ = true;
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
      working_pax_columns_ = GetAllColumns();
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
      auto bm = column->GetBitmap();
      Assert(bm);
      if (!bm->Test(current_row_index_)) {
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

}  // namespace pax
