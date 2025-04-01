#include "storage/columns/pax_columns.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/toast/pax_toast.h"

namespace pax {

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

PaxColumns::PaxColumns()
    : PaxColumn(),
      row_nums_(0),
      storage_format_(PaxStorageFormat::kTypeStoragePorcNonVec) {
  data_ = PAX_NEW<DataBuffer<char>>(0);
}

PaxColumns::~PaxColumns() {
  // Notice that: the resources freed here,
  // must transform owner in `PaxColumns::Merge`
  for (auto column : columns_) {
    PAX_DELETE(column);
  }
  for (auto holder : data_holder_) {
    PAX_DELETE(holder);
  }
  PAX_DELETE(data_);
}

void PaxColumns::SetStorageFormat(PaxStorageFormat format) {
  storage_format_ = format;
}

PaxStorageFormat PaxColumns::GetStorageFormat() const {
  return storage_format_;
}

void PaxColumns::Merge(PaxColumns *columns) {
  Assert(GetColumns() == columns->GetColumns());
  Assert(GetRows() == columns->GetRows());
  Assert(columns->data_holder_.empty());

  for (size_t i = 0; i < columns->GetColumns(); i++) {
    AssertImply(columns_[i], !columns->columns_[i]);
    if (!columns_[i] && columns->columns_[i]) {
      columns_[i] = columns->columns_[i];
      columns->columns_[i] = nullptr;
    }
  }

  if (columns->data_) {
    data_holder_.emplace_back(columns->data_);
    columns->data_ = nullptr;
  }

  PAX_DELETE(columns);
}

PaxColumn *PaxColumns::operator[](uint64 i) { return columns_[i]; }

void PaxColumns::Append(PaxColumn *column) { columns_.emplace_back(column); }

void PaxColumns::Append(char * /*buffer*/, size_t /*size*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

void PaxColumns::AppendToast(char *buffer, size_t size) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

void PaxColumns::Set(DataBuffer<char> *data) {
  Assert(data_->GetBuffer() == nullptr);

  PAX_DELETE(data_);
  data_ = data;
}

size_t PaxColumns::GetNonNullRows() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

int32 PaxColumns::GetTypeLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::PhysicalSize() const {
  size_t total_size = 0;
  for (const auto &column : columns_) {
    if (column) total_size += column->PhysicalSize();
  }
  return total_size;
}

int64 PaxColumns::GetOriginLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

int64 PaxColumns::GetLengthsOriginLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::GetColumns() const { return columns_.size(); }

size_t PaxColumns::ToastCounts() {
  size_t all_number_of_toasts = 0;
  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }
    all_number_of_toasts += column->ToastCounts();
  }

  return all_number_of_toasts;
}

void PaxColumns::SetExternalToastDataBuffer(
    DataBuffer<char> *external_toast_data,
    const std::vector<size_t> &column_sizes) {
  Assert(!external_toast_data_);
  external_toast_data_ = external_toast_data;
  size_t curr_offset = 0;
  for (size_t i = 0; i < columns_.size(); i++) {
    auto column = columns_[i];
    size_t column_size = column_sizes[i];
    // no toasts
    if (!column || column->ToastCounts() == 0) {
      continue;
    }

    // no external toasts
    if (column_size == 0) {
      continue;
    }

    CBDB_CHECK(curr_offset + column_size <= external_toast_data->Used(),
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Invalid toast desc [offset=%lu, size=%lu, toast buffer "
                   "size=%lu]",
                   curr_offset, column_size, external_toast_data->Used()));

    auto column_eb = PAX_NEW<DataBuffer<char>>(
        external_toast_data->Start() + curr_offset, column_size, false, false);
    column_eb->BrushAll();
    column->SetExternalToastDataBuffer(column_eb);
    curr_offset += column_size;
  }
}

DataBuffer<char> *PaxColumns::GetExternalToastDataBuffer() {
  DataBuffer<char> *column_external_buffer;
  size_t buffer_len = 0;

  if (!external_toast_data_) {
    external_toast_data_ = PAX_NEW<DataBuffer<char>>(DEFAULT_CAPACITY);
  }

  if (external_toast_data_->Used() != 0) {
    // already combined
    return external_toast_data_;
  }

  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }
    column_external_buffer = column->GetExternalToastDataBuffer();
    if (!column_external_buffer) {
      continue;
    }
    buffer_len = column_external_buffer->Used();
    if (external_toast_data_->Available() < buffer_len) {
      external_toast_data_->ReSize(external_toast_data_->Used() + buffer_len,
                                   2);
    }

    external_toast_data_->Write(column_external_buffer->Start(), buffer_len);
    external_toast_data_->Brush(buffer_len);
  }

  return external_toast_data_;
}

void PaxColumns::VerifyAllExternalToasts(
    const std::vector<uint64> &ext_toast_lens) {
#ifndef ENABLE_DEBUG
  (void)ext_toast_lens;
#else
  std::vector<std::pair<size_t, size_t>> result_uncombined;
  std::vector<std::pair<size_t, size_t>> result;
  DataBuffer<int32> *data_indexes;
  char *buffer;
  size_t buff_len;

  Assert(ext_toast_lens.size() == columns_.size());

  for (size_t i = 0; i < columns_.size(); i++) {
    auto column = columns_[i];
    if (!column) {
      continue;
    }
    data_indexes = column->GetToastIndexes();
    // no toast here
    if (!data_indexes) {
      Assert(ext_toast_lens[i] == 0);
      continue;
    }

    uint64 off;
    uint64 len;
    bool init = false;
    for (size_t i = 0; i < data_indexes->GetSize(); i++) {
      std::tie(buffer, buff_len) = column->GetBuffer((*data_indexes)[i]);
      Assert(VARATT_IS_PAX_SUPPORT_TOAST(buffer));
      if (!VARATT_IS_EXTERNAL(buffer)) {
        continue;
      }
      Assert(VARTAG_EXTERNAL(buffer) == VARTAG_CUSTOM);

      auto ext_toast = PaxExtGetDatum(buffer);

      if (!init) {
        off = ext_toast->va_extoffs;
        len = ext_toast->va_extsize;
        init = true;
      } else {
        // should no hole in the toast file
        Assert(ext_toast->va_extoffs == (off + len));
        off = ext_toast->va_extoffs;
        len = ext_toast->va_extsize;
      }
    }
  }

  for (size_t i = 0; i < columns_.size(); i++) {
    auto column = columns_[i];
    if (!column) {
      continue;
    }

    data_indexes = column->GetToastIndexes();
    // no toast here
    if (!data_indexes) {
      Assert(ext_toast_lens[i] == 0);
      continue;
    }

    uint64 first_offset;
    bool got_first_external_toast = false;
    for (size_t i = 0; i < data_indexes->GetSize(); i++) {
      std::tie(buffer, buff_len) = column->GetBuffer((*data_indexes)[i]);
      if (VARATT_IS_EXTERNAL(buffer)) {
        auto ext_toast = PaxExtGetDatum(buffer);
        first_offset = ext_toast->va_extoffs;
        got_first_external_toast = true;
        break;
      }
    }

    // exist external toast
    if (got_first_external_toast) {
      uint64 last_offset;
      uint64 last_size;
      for (int64 i = data_indexes->GetSize() - 1; i >= 0; i--) {
        std::tie(buffer, buff_len) = column->GetBuffer((*data_indexes)[i]);
        if (VARATT_IS_EXTERNAL(buffer)) {
          auto ext_toast = PaxExtGetDatum(buffer);
          last_offset = ext_toast->va_extoffs;
          last_size = ext_toast->va_extsize;
          break;
        }
      }
      // check the range is match the len
      Assert(last_offset + last_size - first_offset == ext_toast_lens[i]);
    }
  }
#endif
}

std::pair<char *, size_t> PaxColumns::GetBuffer() {
  PaxColumns::ColumnStreamsFunc column_streams_func_null;
  PaxColumns::ColumnEncodingFunc column_encoding_func_null;
  auto data_buffer =
      GetDataBuffer(column_streams_func_null, column_encoding_func_null);
  return std::make_pair(data_buffer->GetBuffer(), data_buffer->Used());
}

std::pair<char *, size_t> PaxColumns::GetBuffer(size_t position) {
  if (position >= GetColumns()) {
    CBDB_RAISE(
        cbdb::CException::ExType::kExTypeOutOfRange,
        fmt("The [position=%lu] out of [size=%lu] ", position, GetColumns()));
  }
  if (columns_[position]) {
    return columns_[position]->GetBuffer();
  } else {
    return std::make_pair(nullptr, 0);
  }
}

std::pair<char *, size_t> PaxColumns::GetRangeBuffer(size_t /*start_pos*/,
                                                     size_t /*len*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::AlignSize(size_t buf_len, size_t len, size_t align_size) {
  if ((buf_len + len) % align_size != 0) {
    auto align_buf_len = TYPEALIGN(align_size, (buf_len + len));
    Assert(align_buf_len - buf_len > len);
    len = align_buf_len - buf_len;
  }
  return len;
}

DataBuffer<char> *PaxColumns::GetDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  if (data_->GetBuffer() != nullptr) {
    // warning here: better not call GetDataBuffer twice
    // memcpy will happen in GetDataBuffer
    data_->Clear();
  }

#ifdef ENABLE_DEBUG
  auto storage_format = GetStorageFormat();
  for (const auto &column : columns_) {
    AssertImply(column, column->GetStorageFormat() == storage_format);
  }
#endif

  if (COLUMN_STORAGE_FORMAT_IS_VEC(this)) {
    buffer_len =
        MeasureVecDataBuffer(column_streams_func, column_encoding_func);
    data_->Set(BlockBuffer::Alloc<char *>(buffer_len), buffer_len, 0);
    CombineVecDataBuffer();
  } else {
    buffer_len =
        MeasureOrcDataBuffer(column_streams_func, column_encoding_func);
    data_->Set(BlockBuffer::Alloc<char *>(buffer_len), buffer_len, 0);
    CombineOrcDataBuffer();
  }

  Assert(data_->Used() == buffer_len);
  Assert(data_->Available() == 0);
  return data_;
}

size_t PaxColumns::MeasureVecDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;
  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }

    size_t total_rows = column->GetRows();
    size_t non_null_rows = column->GetNonNullRows();

    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      size_t bm_length = bm->MinimalStoredBytes(total_rows);

      bm_length = TYPEALIGN(MEMORY_ALIGN_SIZE, bm_length);
      buffer_len += bm_length;
      column_streams_func(pax::porc::proto::Stream_Kind_PRESENT, total_rows,
                          bm_length, 0);
    }

    if (column->ToastCounts() > 0) {
      auto toast_indexes = column->GetToastIndexes();
      Assert(toast_indexes);
      auto ti_length = TYPEALIGN(MEMORY_ALIGN_SIZE, toast_indexes->Used());
      buffer_len += ti_length;
      column_streams_func(pax::porc::proto::Stream_Kind_TOAST,
                          column->ToastCounts(), ti_length, 0);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeVecBpChar:
      case kTypeVecNoHeader:
      case kTypeNonFixed: {
        size_t padding = 0;
        auto no_fixed_column = reinterpret_cast<PaxVecNonFixedColumn *>(column);
        size_t offsets_size = no_fixed_column->GetOffsetBuffer(true).second;
        padding = TYPEALIGN(MEMORY_ALIGN_SIZE, offsets_size) - offsets_size;
        buffer_len += offsets_size;
        buffer_len += padding;
        column_streams_func(pax::porc::proto::Stream_Kind_LENGTH, total_rows,
                            offsets_size + padding, padding);

        auto data_length = column->GetBuffer().second;
        if (column->GetEncodingType() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          data_length = TYPEALIGN(MEMORY_ALIGN_SIZE, data_length);
        }

        buffer_len += data_length;
        column_streams_func(pax::porc::proto::Stream_Kind_DATA, non_null_rows,
                            data_length, 0);

        break;
      }
      case kTypeVecBitPacked:
      case kTypeVecDecimal:
      case kTypeFixed: {
        auto data_length = column->GetBuffer().second;
        if (column->GetEncodingType() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          data_length = TYPEALIGN(MEMORY_ALIGN_SIZE, data_length);
        }

        buffer_len += data_length;
        column_streams_func(pax::porc::proto::Stream_Kind_DATA, non_null_rows,
                            data_length, 0);
        break;
      }
      case kTypeBitPacked:
      case kTypeDecimal:
      case kTypeBpChar:
      case kTypeInvalid:
      default: {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeLogicError,
            fmt("Invalid column type = %d", column->GetPaxColumnTypeInMem()));
        break;
      }
    }

    AssertImply(column->GetEncodingType() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                column->GetOriginLength() >= 0);
    AssertImply(column->GetLengthsEncodingType() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                column->GetLengthsOriginLength() >= 0);

    // length origin lengths
    column_encoding_func(
        column->GetEncodingType(), column->GetCompressLevel(),
        (column->GetEncodingType() !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED)
            ? TYPEALIGN(MEMORY_ALIGN_SIZE, column->GetOriginLength())
            : column->GetOriginLength(),
        column->GetLengthsEncodingType(), column->GetLengthsCompressLevel(),
        (column->GetLengthsEncodingType() !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED)
            ? TYPEALIGN(MEMORY_ALIGN_SIZE, column->GetLengthsOriginLength())
            : column->GetLengthsOriginLength());
  }
  return buffer_len;
}

size_t PaxColumns::MeasureOrcDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }

    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      size_t bm_length = bm->MinimalStoredBytes(column->GetRows());
      buffer_len += bm_length;
      column_streams_func(pax::porc::proto::Stream_Kind_PRESENT,
                          column->GetRows(), bm_length, 0);
    }

    if (column->ToastCounts() > 0) {
      auto toast_indexes = column->GetToastIndexes();
      Assert(toast_indexes);
      auto ti_length = toast_indexes->Used();
      buffer_len += ti_length;
      column_streams_func(pax::porc::proto::Stream_Kind_TOAST,
                          column->ToastCounts(), ti_length, 0);
    }

    size_t column_size = column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeBpChar:
      case kTypeDecimal:
      case kTypeNonFixed: {
        size_t length_stream_size =
            ((PaxNonFixedColumn *)column)->GetLengthBuffer().second;

        size_t padding = 0;

        if ((buffer_len + length_stream_size) % column->GetAlignSize() != 0) {
          auto align_buffer_len = TYPEALIGN(column->GetAlignSize(),
                                            (buffer_len + length_stream_size));
          Assert(align_buffer_len - buffer_len > length_stream_size);
          padding = align_buffer_len - buffer_len - length_stream_size;
        }

        buffer_len += length_stream_size;
        buffer_len += padding;
        column_streams_func(pax::porc::proto::Stream_Kind_LENGTH, column_size,
                            length_stream_size + padding, padding);

        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;

        column_streams_func(pax::porc::proto::Stream_Kind_DATA, column_size,
                            length_data, 0);

        break;
      }
      case kTypeBitPacked:
      case kTypeFixed: {
        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;
        column_streams_func(pax::porc::proto::Stream_Kind_DATA, column_size,
                            length_data, 0);

        break;
      }
      case kTypeVecBitPacked:
      case kTypeVecDecimal:
      case kTypeVecBpChar:
      case kTypeVecNoHeader:
      case kTypeInvalid:
      default: {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeLogicError,
            fmt("Invalid column type = %d", column->GetPaxColumnTypeInMem()));
        break;
      }
    }

    column_encoding_func(
        column->GetEncodingType(), column->GetCompressLevel(),
        column->GetOriginLength(), column->GetLengthsEncodingType(),
        column->GetLengthsCompressLevel(), column->GetLengthsOriginLength());
  }
  return buffer_len;
}

void PaxColumns::CombineVecDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  auto fill_padding_buffer = [](PaxColumn *column,
                                DataBuffer<char> *data_buffer,
                                size_t buffer_len, size_t align) {
    Assert(data_buffer);

    if (column && (column->GetEncodingType() !=
                   ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED)) {
      return;
    }

    auto gap_size = TYPEALIGN(align, buffer_len) - buffer_len;
    if (!gap_size) {
      return;
    }

    data_buffer->WriteZero(gap_size);
    data_buffer->Brush(gap_size);
  };

  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }

    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      auto nbytes = bm->MinimalStoredBytes(column->GetRows());
      Assert(nbytes <= bm->Raw().size);

      Assert(data_->Available() >= nbytes);
      data_->Write(reinterpret_cast<char *>(bm->Raw().bitmap), nbytes);
      data_->Brush(nbytes);

      fill_padding_buffer(nullptr, data_, nbytes, MEMORY_ALIGN_SIZE);
    }

    if (column->ToastCounts() > 0) {
      auto toast_indexes = column->GetToastIndexes();
      Assert(toast_indexes);
      auto nbytes = toast_indexes->Used();

      Assert(data_->Available() >= nbytes);

      data_->Write(reinterpret_cast<char *>(toast_indexes->Start()), nbytes);
      data_->Brush(nbytes);
      fill_padding_buffer(nullptr, data_, nbytes, MEMORY_ALIGN_SIZE);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeVecBpChar:
      case kTypeVecNoHeader:
      case kTypeNonFixed: {
        char *length_stream_data;
        size_t length_stream_len;
        auto no_fixed_column = reinterpret_cast<PaxVecNonFixedColumn *>(column);
        std::tie(length_stream_data, length_stream_len) =
            no_fixed_column->GetOffsetBuffer(false);

        Assert(data_->Available() >= length_stream_len);
        data_->Write(length_stream_data, length_stream_len);
        data_->Brush(length_stream_len);

        fill_padding_buffer(nullptr, data_, length_stream_len,
                            MEMORY_ALIGN_SIZE);

        std::tie(buffer, buffer_len) = column->GetBuffer();
        Assert(data_->Available() >= buffer_len);
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        fill_padding_buffer(no_fixed_column, data_, buffer_len,
                            MEMORY_ALIGN_SIZE);

        break;
      }
      case kTypeVecBitPacked:
      case kTypeVecDecimal:
      case kTypeFixed: {
        std::tie(buffer, buffer_len) = column->GetBuffer();

        Assert(data_->Available() >= buffer_len);
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        fill_padding_buffer(column, data_, buffer_len, MEMORY_ALIGN_SIZE);
        break;
      }
      case kTypeBitPacked:
      case kTypeDecimal:
      case kTypeBpChar:
      case kTypeInvalid:
      default: {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeLogicError,
            fmt("Invalid column type = %d", column->GetPaxColumnTypeInMem()));
        break;
      }
    }
  }
}

void PaxColumns::CombineOrcDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  for (const auto &column : columns_) {
    if (!column) {
      continue;
    }

    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      auto nbytes = bm->MinimalStoredBytes(column->GetRows());
      Assert(nbytes <= bm->Raw().size);

      Assert(data_->Available() >= nbytes);
      data_->Write(reinterpret_cast<char *>(bm->Raw().bitmap), nbytes);
      data_->Brush(nbytes);
    }

    if (column->ToastCounts() > 0) {
      auto toast_indexes = column->GetToastIndexes();
      Assert(toast_indexes);
      auto nbytes = toast_indexes->Used();

      Assert(data_->Available() >= nbytes);
      data_->Write(reinterpret_cast<char *>(toast_indexes->Start()), nbytes);
      data_->Brush(nbytes);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeBpChar:
      case kTypeDecimal:
      case kTypeNonFixed: {
        char *length_stream_data;
        size_t length_stream_len;
        size_t length_stream_aligned_len;
        auto no_fixed_column = reinterpret_cast<PaxNonFixedColumn *>(column);
        std::tie(length_stream_data, length_stream_len) =
            no_fixed_column->GetLengthBuffer();

        auto current_buffer_len = data_->Used();
        if ((current_buffer_len + length_stream_len) % column->GetAlignSize() !=
            0) {
          auto align_buffer_len = TYPEALIGN(
              column->GetAlignSize(), (current_buffer_len + length_stream_len));
          Assert(align_buffer_len - current_buffer_len > length_stream_len);
          length_stream_aligned_len = align_buffer_len - current_buffer_len;
        } else {
          length_stream_aligned_len = length_stream_len;
        }

        memcpy(data_->GetAvailableBuffer(), length_stream_data,
               length_stream_len);
        data_->Brush(length_stream_len);

        Assert(length_stream_aligned_len >= length_stream_len);
        if (length_stream_aligned_len > length_stream_len) {
          auto padding = length_stream_aligned_len - length_stream_len;
          data_->WriteZero(padding);
          data_->Brush(padding);
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        break;
      }
      case kTypeBitPacked:
      case kTypeFixed: {
        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);
        break;
      }
      case kTypeVecBitPacked:
      case kTypeVecDecimal:
      case kTypeVecBpChar:
      case kTypeVecNoHeader:
      case kTypeInvalid:
      default: {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeLogicError,
            fmt("Invalid column type = %d", column->GetPaxColumnTypeInMem()));
        break;
      }
    }
  }
}

}  //  namespace pax
