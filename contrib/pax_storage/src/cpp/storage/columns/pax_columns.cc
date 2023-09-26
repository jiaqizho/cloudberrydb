#include "storage/columns/pax_columns.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/columns/pax_column_int.h"
#include "storage/columns/pax_encoding_column.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax {

PaxColumns::PaxColumns(const std::vector<orc::proto::Type_Kind> &types,
                       const std::vector<std::tuple<ColumnEncoding_Kind, int>>
                           &column_encoding_types)
    : row_nums_(0) {
  Assert(types.size() == column_encoding_types.size());
  data_ = new DataBuffer<char>(0);
  for (size_t i = 0; i < types.size(); i++) {
    auto type = types[i];
    switch (type) {
      case (orc::proto::Type_Kind::Type_Kind_STRING): {
        PaxEncoder::EncodingOption encoding_option;
        encoding_option.column_encode_type =
            std::get<0>(column_encoding_types[i]);
        encoding_option.is_sign = false;
        encoding_option.compress_level = std::get<1>(column_encoding_types[i]);

        auto pax_non_fixed_column = new PaxNonFixedEncodingColumn(  //
            DEFAULT_CAPACITY, std::move(encoding_option));
        // current memory will copy from tuple, so should take over it
        pax_non_fixed_column->SetMemTakeOver(true);
        columns_.emplace_back(pax_non_fixed_column);
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_BOOLEAN):
      case (orc::proto::Type_Kind::Type_Kind_BYTE): {  // len 1 integer
        columns_.emplace_back(new PaxCommColumn<char>());
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_SHORT): {  // len 2 integer
        PaxEncoder::EncodingOption encoding_option;
        encoding_option.column_encode_type =
            std::get<0>(column_encoding_types[i]);
        encoding_option.is_sign = true;
        encoding_option.compress_level = std::get<1>(column_encoding_types[i]);
        columns_.emplace_back(
            new PaxIntColumn<int16>(std::move(encoding_option)));
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_INT): {  // len 4 integer
        PaxEncoder::EncodingOption encoding_option;
        encoding_option.column_encode_type =
            std::get<0>(column_encoding_types[i]);
        encoding_option.is_sign = true;
        encoding_option.compress_level = std::get<1>(column_encoding_types[i]);
        columns_.emplace_back(
            new PaxIntColumn<int32>(std::move(encoding_option)));
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_LONG): {  // len 8 integer
        PaxEncoder::EncodingOption encoding_option;
        encoding_option.column_encode_type =
            std::get<0>(column_encoding_types[i]);
        encoding_option.is_sign = true;
        encoding_option.compress_level = std::get<1>(column_encoding_types[i]);
        columns_.emplace_back(
            new PaxIntColumn<int64>(std::move(encoding_option)));

        break;
      }
      default:
        // TODO(jiaqizho): support other column type
        // but now should't be here
        Assert(!"non-implemented column type");
        break;
    }
  }
}

PaxColumns::PaxColumns() : row_nums_(0) { data_ = new DataBuffer<char>(0); }

PaxColumns::~PaxColumns() {
  // Notice that: the resources freed here,
  // must transform owner in `PaxColumns::Merge`
  for (auto column : columns_) {
    delete column;
  }
  for (auto holder : data_holder_) {
    delete holder;
  }
  delete data_;
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

  delete columns;
}

PaxColumn *PaxColumns::operator[](uint64 i) { return columns_[i]; }

void PaxColumns::Append(PaxColumn *column) { columns_.emplace_back(column); }

void PaxColumns::Append([[maybe_unused]] char *buffer,
                        [[maybe_unused]] size_t size) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

void PaxColumns::Set(DataBuffer<char> *data) {
  Assert(data_->GetBuffer() == nullptr);

  delete data_;
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
  for (auto column : columns_) {
    if (column) total_size += column->PhysicalSize();
  }
  return total_size;
}

int64 PaxColumns::GetOriginLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::GetColumns() const { return columns_.size(); }

std::pair<char *, size_t> PaxColumns::GetBuffer() {
  PaxColumns::ColumnStreamsFunc column_streams_func_null;
  PaxColumns::ColumnEncodingFunc column_encoding_func_null;
  auto data_buffer =
      GetDataBuffer(column_streams_func_null, column_encoding_func_null);
  return std::make_pair(data_buffer->GetBuffer(), data_buffer->Used());
}

std::pair<char *, size_t> PaxColumns::GetBuffer(size_t position) {
  if (position >= GetColumns()) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeOutOfRange);
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

DataBuffer<char> *PaxColumns::GetDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  if (data_->GetBuffer() != nullptr) {
    // warning here: better not call GetDataBuffer twice
    // memcpy will happen in GetDataBuffer
    data_->Clear();
  }

  buffer_len = MeasureDataBuffer(column_streams_func, column_encoding_func);
  data_->Set(reinterpret_cast<char *>(cbdb::Palloc(buffer_len)), buffer_len, 0);
  CombineDataBuffer();
  return data_;
}

size_t PaxColumns::MeasureDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      size_t bm_length = bm->MinimalStoredBytes(column->GetRows());
      buffer_len += bm_length;
      column_streams_func(orc::proto::Stream_Kind_PRESENT, column->GetRows(),
                          bm_length);
    }

    size_t column_size = column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        size_t lengths_size = column_size * sizeof(int64);

        buffer_len += lengths_size;
        column_streams_func(orc::proto::Stream_Kind_LENGTH, column_size,
                            lengths_size);

        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;

        column_streams_func(orc::proto::Stream_Kind_DATA, column_size,
                            length_data);

        break;
      }
      case kTypeFixed: {
        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;
        column_streams_func(orc::proto::Stream_Kind_DATA, column_size,
                            length_data);

        break;
      }
      case kTypeInvalid:
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
        break;
      }
    }

    column_encoding_func(column->GetEncodingType(), column->GetOriginLength());
  }
  return buffer_len;
}

void PaxColumns::CombineDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      auto nbytes = bm->MinimalStoredBytes(column->GetRows());
      Assert(nbytes <= bm->Raw().size);

      data_->Write(reinterpret_cast<char *>(bm->Raw().bitmap), nbytes);
      data_->Brush(nbytes);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        auto no_fixed_column = reinterpret_cast<PaxNonFixedColumn *>(column);
        auto length_data_buffer = no_fixed_column->GetLengthBuffer();

        memcpy(data_->GetAvailableBuffer(), length_data_buffer->GetBuffer(),
               length_data_buffer->Used());
        data_->Brush(length_data_buffer->Used());

        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        break;
      }
      case kTypeFixed: {
        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);
        break;
      }
      case kTypeInvalid:
      default:
        break;
    }
  }
}
}  //  namespace pax
