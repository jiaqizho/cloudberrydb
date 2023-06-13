#include "storage/pax_column.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "comm/pax_defer.h"
#include "exceptions/CException.h"

namespace pax {

PaxColumn::PaxColumn() : null_bitmap_(nullptr), is_encoded_(false) {}

PaxColumn::~PaxColumn() {
  if (null_bitmap_) {
    delete null_bitmap_;
  }
}

PaxColumnTypeInMem PaxColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::TYPE_INVALID;
}

void PaxColumn::Clear() {
  if (null_bitmap_) {
    delete null_bitmap_;
    null_bitmap_ = nullptr;
  }
}

bool PaxColumn::HasNull() { return null_bitmap_ != nullptr; }

void PaxColumn::SetNulls(DataBuffer<bool> *null_bitmap) {
  Assert(!null_bitmap_);
  null_bitmap_ = null_bitmap;
}

DataBuffer<bool> *PaxColumn::GetNulls() const { return null_bitmap_; }

size_t PaxColumn::GetRows() {
  return null_bitmap_ ? null_bitmap_->Used() : GetNonNullRows();
}

void PaxColumn::AppendNull() {
  if (!null_bitmap_) {
    size_t current_rows = GetNonNullRows();
    size_t size = current_rows > DEFAULT_CAPACITY
                      ? (current_rows / DEFAULT_CAPACITY + 1) * DEFAULT_CAPACITY
                      : DEFAULT_CAPACITY;
    null_bitmap_ = new DataBuffer<bool>(size);
    null_bitmap_->Brush(current_rows * sizeof(bool));
    memset(null_bitmap_->GetBuffer(), 1, null_bitmap_->Capacity());
  }

  if (null_bitmap_->Available() == 0) {
    size_t old_cap = null_bitmap_->Capacity();
    null_bitmap_->ReSize(old_cap * 2);
    memset(null_bitmap_->GetAvailableBuffer(), 1, old_cap);
  }

  null_bitmap_->Write(false);
  null_bitmap_->Brush(sizeof(bool));
}

void PaxColumn::Append([[maybe_unused]] char *buffer,
                       [[maybe_unused]] size_t size) {
  if (null_bitmap_) {
    if (null_bitmap_->Available() == 0) {
      size_t old_cap = null_bitmap_->Capacity();
      null_bitmap_->ReSize(old_cap * 2);
      memset(null_bitmap_->GetAvailableBuffer(), 1, old_cap);
    }
    null_bitmap_->Brush(sizeof(bool));
  }
}

template <typename T>
PaxCommColumn<T>::PaxCommColumn(uint64 capacity)
    : PaxColumn(), capacity_(capacity) {
  data_ = new DataBuffer<T>(capacity * sizeof(T));
}

template <typename T>
PaxCommColumn<T>::~PaxCommColumn() {
  delete data_;
}

template <typename T>
PaxCommColumn<T>::PaxCommColumn() : PaxCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxCommColumn<T>::Set(DataBuffer<T> *data) {
  if (data_) {
    delete data_;
  }

  data_ = data;
}

template <typename T>
void PaxCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  auto *buffer_t = reinterpret_cast<T *>(buffer);

  Assert(size == sizeof(T));
  Assert(GetNonNullRows() <= capacity_);

  if (GetNonNullRows() == capacity_) {
    ReSize(capacity_ * 2);
  }

  data_->Write(buffer_t, sizeof(T));
  data_->Brush(sizeof(T));
}

template <typename T>
PaxColumnTypeInMem PaxCommColumn<T>::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::TYPE_FIXED;
}

template <typename T>
DataBuffer<T> *PaxCommColumn<T>::GetDataBuffer() {
  return data_;
}

template <typename T>
void PaxCommColumn<T>::Clear() {
  PaxColumn::Clear();
  data_->BrushBackAll();
}

template <typename T>
void PaxCommColumn<T>::ReSize(uint64 cap) {
  if (capacity_ < cap) {
    data_->ReSize(cap * sizeof(T));
    capacity_ = cap;
  }
}

template <typename T>
size_t PaxCommColumn<T>::GetNonNullRows() const {
  return data_->Used() / sizeof(T);
}

template <typename T>
size_t PaxCommColumn<T>::EstimatedSize() const {
  return data_->Used();
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Buffer().Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer(size_t position) {
  if (position >= GetNonNullRows()) {
    CBDB_RAISE(cbdb::CException::ExType::ExTypeOutOfRange);
  }
  return std::make_pair(data_->Buffer().Start() + (sizeof(T) * position),
                        sizeof(T));
}

template class PaxCommColumn<char>;
template class PaxCommColumn<int16>;
template class PaxCommColumn<int32>;
template class PaxCommColumn<int64>;
template class PaxCommColumn<float>;
template class PaxCommColumn<double>;

PaxNonFixedColumn::PaxNonFixedColumn(uint64 capacity)
    : PaxColumn(), estimated_size_(0) {
  data_ = new DataBuffer<char>(capacity * sizeof(char) * 100);
  lengths_ = new DataBuffer<int64>(capacity * sizeof(char));
}

PaxNonFixedColumn::PaxNonFixedColumn() : PaxNonFixedColumn(DEFAULT_CAPACITY) {}

PaxNonFixedColumn::~PaxNonFixedColumn() {
  if (data_) delete data_;
  if (lengths_) delete lengths_;
}

void PaxNonFixedColumn::Set(DataBuffer<char> *data, DataBuffer<int64> *lengths,
                            size_t total_size) {
  if (data_) delete data_;
  if (lengths_) delete lengths_;

  estimated_size_ = total_size;
  data_ = data;
  lengths_ = lengths;
  offsets_.clear();
  for (size_t i = 0; i < lengths_->GetSize(); i++) {
    offsets_.emplace_back(i == 0 ? 0 : offsets_[i - 1] + (*lengths_)[i - 1]);
  }
}

void PaxNonFixedColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  while (data_->Available() < size) {
    data_->ReSize(data_->Capacity() * 2);
  }

  if (lengths_->Available() == 0) {
    lengths_->ReSize(lengths_->Capacity() * 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, size);
  data_->Brush(size);
  lengths_->Write(reinterpret_cast<int64 *>(&size), sizeof(int64));
  lengths_->Brush(sizeof(int64));

  offsets_.emplace_back(offsets_.empty()
                            ? 0
                            : offsets_[offsets_.size() - 1] +
                                  (*lengths_)[offsets_.size() - 1]);
  Assert(offsets_.size() == lengths_->GetSize());
}

DataBuffer<int64> *PaxNonFixedColumn::GetLengthBuffer() const {
  return lengths_;
}

PaxColumnTypeInMem PaxNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::TYPE_NON_FIXED;
}

void PaxNonFixedColumn::Clear() {
  PaxColumn::Clear();

  data_->BrushBackAll();
  lengths_->BrushBackAll();

  offsets_.clear();
}

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer() {
  return std::make_pair(data_->GetBuffer(), data_->Used());
}

size_t PaxNonFixedColumn::GetNonNullRows() const { return lengths_->GetSize(); }

size_t PaxNonFixedColumn::EstimatedSize() const { return estimated_size_; }

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer(size_t position) {
  if (position >= GetNonNullRows()) {
    CBDB_RAISE(cbdb::CException::ExType::ExTypeOutOfRange);
  }

  return std::make_pair(data_->GetBuffer() + offsets_[position],
                        (*lengths_)[position]);
}

bool PaxNonFixedColumn::IsMemTakeOver() const {
  Assert(data_->IsMemTakeOver() == lengths_->IsMemTakeOver());
  return data_->IsMemTakeOver();
}

void PaxNonFixedColumn::SetMemTakeOver(bool take_over) {
  data_->SetMemTakeOver(take_over);
  lengths_->SetMemTakeOver(take_over);
}

PaxColumns::PaxColumns(std::vector<orc::proto::Type_Kind> types) : PaxColumn() {
  data_ = new DataBuffer<char>(0);
  for (auto &type : types) {
    switch (type) {
      case (orc::proto::Type_Kind::Type_Kind_STRING): {
        auto *pax_non_fixed_column = new PaxNonFixedColumn();
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
        columns_.emplace_back(new PaxCommColumn<int16>());
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_INT): {  // len 4 integer
        columns_.emplace_back(new PaxCommColumn<int32>());
        break;
      }
      case (orc::proto::Type_Kind::Type_Kind_LONG): {
        columns_.emplace_back(new PaxCommColumn<int64>());  // len 8 integer
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

PaxColumns::PaxColumns() : PaxColumn() { data_ = new DataBuffer<char>(0); }

PaxColumns::~PaxColumns() {
  for (auto *column : columns_) {
    delete column;
  }
  delete data_;
}

void PaxColumns::Clear() {
  for (auto *column : columns_) {
    column->Clear();
  }

  data_->Clear();
}

PaxColumn *PaxColumns::operator[](uint64 i) { return columns_[i]; }

void PaxColumns::Append(PaxColumn *column) { columns_.emplace_back(column); }

void PaxColumns::Append([[maybe_unused]] char *buffer,
                        [[maybe_unused]] size_t size) {
  CBDB_RAISE(cbdb::CException::ExType::ExTypeLogicError);
}

void PaxColumns::Set(DataBuffer<char> *data) {
  Assert(data_->GetBuffer() == nullptr);

  delete data_;
  data_ = data;
}

size_t PaxColumns::GetNonNullRows() const {
  CBDB_RAISE(cbdb::CException::ExType::ExTypeLogicError);
}

size_t PaxColumns::EstimatedSize() const {
  size_t total_size = 0;
  for (auto *column : columns_) {
    total_size += column->EstimatedSize();
  }
  return total_size;
}

size_t PaxColumns::GetColumns() const { return columns_.size(); }

std::pair<char *, size_t> PaxColumns::GetBuffer() {
  PaxColumns::PreCalcBufferFunc func_null;
  auto *data_buffer = GetDataBuffer(func_null);
  return std::make_pair(data_buffer->GetBuffer(), data_buffer->Used());
}

std::pair<char *, size_t> PaxColumns::GetBuffer(size_t position) {
  if (position >= GetColumns()) {
    CBDB_RAISE(cbdb::CException::ExType::ExTypeOutOfRange);
  }
  return columns_[position]->GetBuffer();
}

DataBuffer<char> *PaxColumns::GetDataBuffer(const PreCalcBufferFunc &func) {
  size_t buffer_len = 0;

  if (data_->GetBuffer() != nullptr) {
    // warning here: better not call GetDataBuffer twice
    // memcpy will happen in GetDataBuffer
    data_->Clear();
  }

  buffer_len = MeasureDataBuffer(func);
  data_->Set(reinterpret_cast<char *>(cbdb::Palloc(buffer_len)), buffer_len, 0);
  CombineDataBuffer();
  return data_;
}

size_t PaxColumns::MeasureDataBuffer(const PreCalcBufferFunc &pre_calc_func) {
  size_t buffer_len = 0;

  for (auto *column : columns_) {
    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      size_t non_null_length = column->GetNulls()->Used();
      buffer_len += non_null_length;
      pre_calc_func(orc::proto::Stream_Kind_PRESENT, column->GetRows(),
                    non_null_length);
    }

    size_t column_size = column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case TYPE_NON_FIXED: {
        size_t lengths_size = column_size * sizeof(int64);

        buffer_len += lengths_size;
        pre_calc_func(orc::proto::Stream_Kind_LENGTH, column_size,
                      lengths_size);

        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;

        pre_calc_func(orc::proto::Stream_Kind_DATA, column_size, length_data);

        break;
      }
      case TYPE_FIXED: {
        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;
        pre_calc_func(orc::proto::Stream_Kind_DATA, column_size, length_data);

        break;
      }
      case TYPE_INVALID:
      default: {
        CBDB_RAISE(cbdb::CException::ExType::ExTypeLogicError);
        break;
      }
    }
  }
  return buffer_len;
}

void PaxColumns::CombineDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  Assert(data_->Capacity() != 0);

  for (auto *column : columns_) {
    if (column->HasNull()) {
      auto null_data_buffer = column->GetNulls();
      size_t non_null_length = null_data_buffer->Used();

      data_->Write(reinterpret_cast<char *>(null_data_buffer->GetBuffer()),
                   non_null_length);
      data_->Brush(non_null_length);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case TYPE_NON_FIXED: {
        PaxNonFixedColumn *no_fixed_column =
            reinterpret_cast<PaxNonFixedColumn *>(column);
        auto length_data_buffer = no_fixed_column->GetLengthBuffer();

        memcpy(data_->GetAvailableBuffer(), length_data_buffer->GetBuffer(),
               length_data_buffer->Used());
        data_->Brush(length_data_buffer->Used());

        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        break;
      }
      case TYPE_FIXED: {
        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);
        break;
      }
      case TYPE_INVALID:
      default:
        break;
    }
  }
}

};  // namespace pax
