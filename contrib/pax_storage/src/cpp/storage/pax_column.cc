#include "storage/pax_column.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "comm/pax_defer.h"
#include "exceptions/CException.h"

namespace pax {

PaxColumn::PaxColumn() : has_nulls_(false), is_encoded_(false) {}

PaxColumnTypeInMem PaxColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::TYPE_INVALID;
}

void PaxColumn::Clear() { non_null_.clear(); }

bool PaxColumn::HasNull() { return has_nulls_; }

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
  auto *buffer_t = reinterpret_cast<T *>(buffer);

  Assert(size == sizeof(T));
  Assert(GetRows() <= capacity_);

  if (GetRows() == capacity_) {
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
size_t PaxCommColumn<T>::GetRows() const {
  return data_->Used() / sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Buffer().Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer(size_t position) {
  if (position >= GetRows()) {
    CBDB_RAISE(cbdb::CException::ExType::ExTypeOutOfRange);
  }
  return std::make_pair(data_->Buffer().Start() + (sizeof(T) * position),
                        sizeof(T));
}

template class PaxCommColumn<char>;
template class PaxCommColumn<int32>;

PaxNonFixedColumn::PaxNonFixedColumn() : PaxColumn() {
  data_ = new std::vector<DataBuffer<char> *>();
}

PaxNonFixedColumn::~PaxNonFixedColumn() {
  for (auto *data_buffer : *data_) {
    data_buffer->SetMemTakeOver(mem_take_over_);
    delete data_buffer;
  }
  delete data_;
}

void PaxNonFixedColumn::Set(std::vector<DataBuffer<char> *> *data) {
  if (data_) {
    for (auto *data_buffer : *data_) {
      data_buffer->SetMemTakeOver(mem_take_over_);
      delete data_buffer;
    }
    delete data_;
  }

  data_ = data;
}

void PaxNonFixedColumn::Append(char *buffer, size_t size) {
  // TODO(jiaqizho): consider use DataBuffer<char> replace std::vector<DataBuffer<char> *>
  // Cause we can't direct hold the ptr which from tuple
  DataBuffer<char> *data_buffer = new DataBuffer<char>(nullptr, size, false, true);

  data_buffer->Write(buffer, size);
  data_buffer->Brush(size);
  data_->emplace_back(data_buffer);
}

PaxColumnTypeInMem PaxNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::TYPE_NON_FIXED;
}

void PaxNonFixedColumn::Clear() {
  PaxColumn::Clear();

  for (auto *data_buffer : *data_) {
    data_buffer->SetMemTakeOver(mem_take_over_);
    delete data_buffer;
  }
  delete data_;
  data_ = new std::vector<DataBuffer<char> *>();
}

std::vector<DataBuffer<char> *> *PaxNonFixedColumn::GetDataBuffers() {
  return data_;
}

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer() {
  // should not direct call this function
  CBDB_RAISE(cbdb::CException::ExType::ExTypeAssert);
}

size_t PaxNonFixedColumn::GetRows() const { return data_->size(); }

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer(size_t position) {
  if (position >= GetRows()) {
    CBDB_RAISE(cbdb::CException::ExType::ExTypeOutOfRange);
  }
  auto *data_buffer = (*data_)[position];
  return std::make_pair(data_buffer->GetBuffer(), data_buffer->Used());
}

bool PaxNonFixedColumn::IsMemTakeOver() const { return mem_take_over_; }

void PaxNonFixedColumn::SetMemTakeOver(bool take_over) {
  mem_take_over_ = take_over;
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
      case (orc::proto::Type_Kind::Type_Kind_INT): {  // len 4
        columns_.emplace_back(new PaxCommColumn<int32>());
      }
      default:
        // TODO(jiaqizho): support other column type
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

size_t PaxColumns::GetRows() const {
  CBDB_RAISE(cbdb::CException::ExType::ExTypeLogicError);
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
    size_t column_size = column->GetRows();
    switch (column->GetPaxColumnTypeInMem()) {
      case TYPE_NON_FIXED: {
        size_t lengths_size = column_size * sizeof(int64);

        buffer_len += lengths_size;
        pre_calc_func(orc::proto::Stream_Kind_LENGTH, column_size,
                      lengths_size);

        auto length_data = 0;
        for (size_t i = 0; i < column_size; i++) {
          length_data += column->GetBuffer(i).second;
        }
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
  const char *data = nullptr;
  size_t buffer_len = 0;

  Assert(data_->Capacity() != 0);

  for (auto *column : columns_) {
    switch (column->GetPaxColumnTypeInMem()) {
      case TYPE_NON_FIXED: {
        size_t column_size = column->GetRows();
        auto *length_data_buffer =
            new DataBuffer<int64>(column_size * sizeof(int64));

        defer({ delete length_data_buffer; });

        auto *length_ptr = data_->GetAvailableBuffer();
        data_->Brush(length_data_buffer->Capacity());

        for (size_t i = 0; i < column_size; i++) {
          std::tie(data, buffer_len) = column->GetBuffer(i);
          length_data_buffer->Write(reinterpret_cast<int64 *>(&buffer_len),
                                    sizeof(int64));
          length_data_buffer->Brush(sizeof(int64));

          data_->Write(data, buffer_len);
          data_->Brush(buffer_len);
        }

        Assert(length_data_buffer->Used() == length_data_buffer->Capacity());
        memcpy(length_ptr, length_data_buffer->GetBuffer(),
               length_data_buffer->Used());

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
