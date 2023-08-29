#include "storage/columns/pax_column.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "comm/pax_defer.h"

namespace pax {

PaxColumn::PaxColumn()
    : null_bitmap_(nullptr),
      encoded_type_(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
      storage_type_(PaxColumnStorageType::kTypeStorageNonVec) {}

PaxColumn::~PaxColumn() {
  if (null_bitmap_) {
    delete null_bitmap_;
  }
}

PaxColumnTypeInMem PaxColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeInvalid;
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

std::pair<bool *, size_t> PaxColumn::GetRangeNulls(size_t start_pos,
                                                   size_t len) {
  Assert(null_bitmap_);
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);

  static_assert(sizeof(char) == sizeof(bool));
  return std::make_pair(null_bitmap_->GetBuffer() + start_pos, len);
}

size_t PaxColumn::GetRows() {
  return null_bitmap_ ? null_bitmap_->Used() : GetNonNullRows();
}

size_t PaxColumn::GetRangeNonNullRows(size_t start_pos, size_t len) {
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  if (null_bitmap_) {
    size_t total_non_null = 0;
    for (size_t i = start_pos; i < (start_pos + len); i++) {
      if ((*null_bitmap_)[i]) {
        total_non_null++;
      }
    }

    return total_non_null;
  } else {
    return len;
  }
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

PaxColumn *PaxColumn::SetColumnEncodeType(ColumnEncoding_Kind encoding_type) {
  encoded_type_ = encoding_type;
  return this;
}

PaxColumn *PaxColumn::SetColumnStorageType(PaxColumnStorageType storage_type) {
  storage_type_ = storage_type;
  return this;
}

ColumnEncoding_Kind PaxColumn::GetEncodingType() const { return encoded_type_; }

template <typename T>
PaxCommColumn<T>::PaxCommColumn(uint64 capacity) : capacity_(capacity) {
  data_ = new DataBuffer<T>(capacity * sizeof(T));
}

template <typename T>
PaxCommColumn<T>::~PaxCommColumn() {
  delete data_;
}

template <typename T>  // NOLINT: redirect constructor
PaxCommColumn<T>::PaxCommColumn() : PaxCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxCommColumn<T>::Set(DataBuffer<T> *data) {
  delete data_;

  data_ = data;
}

template <typename T>
void PaxCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  auto buffer_t = reinterpret_cast<T *>(buffer);

  // TODO(jiaqizho): Is it necessary to support multiple buffer insertions for
  // bulk insert push to mirco partition?
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
  return PaxColumnTypeInMem::kTypeFixed;
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
size_t PaxCommColumn<T>::PhysicalSize() const {
  return data_->Used();
}

template <typename T>
int64 PaxCommColumn<T>::GetOriginLength() const {
  return NO_ENCODE_ORIGIN_LEN;
}

template <typename T>
int32 PaxCommColumn<T>::GetTypeLength() const {
  return sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * position), sizeof(T));
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetRangeBuffer(size_t start_pos,
                                                           size_t len) {
  CBDB_CHECK((start_pos + len) <= GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * start_pos),
                        sizeof(T) * len);
}

template class PaxCommColumn<char>;
template class PaxCommColumn<int8>;
template class PaxCommColumn<int16>;
template class PaxCommColumn<int32>;
template class PaxCommColumn<int64>;
template class PaxCommColumn<float>;
template class PaxCommColumn<double>;

PaxNonFixedColumn::PaxNonFixedColumn(uint64 capacity) : estimated_size_(0) {
  data_ = new DataBuffer<char>(capacity * sizeof(char) * 100);
  lengths_ = new DataBuffer<int64>(capacity * sizeof(char));
}

PaxNonFixedColumn::PaxNonFixedColumn() : PaxNonFixedColumn(DEFAULT_CAPACITY) {}

PaxNonFixedColumn::~PaxNonFixedColumn() {
  if (data_) {
    delete data_;
  }

  if (lengths_) {
    delete lengths_;
  }
}

void PaxNonFixedColumn::Set(DataBuffer<char> *data, DataBuffer<int64> *lengths,
                            size_t total_size) {
  if (data_) {
    delete data_;
  }

  if (lengths_) {
    delete lengths_;
  }

  estimated_size_ = total_size;
  data_ = data;
  lengths_ = lengths;
  offsets_.clear();
  for (size_t i = 0; i < lengths_->GetSize(); i++) {
    offsets_.emplace_back(i == 0 ? 0 : offsets_[i - 1] + (*lengths_)[i - 1]);
  }
}

void PaxNonFixedColumn::Append(char *buffer, size_t size) {
  Assert(likely(reinterpret_cast<char *> MAXALIGN(data_->Position()) ==
                data_->Position()));

  size_t origin_size;
  origin_size = size;

  // FIMXE(gongxun): maybe it should be aligned base on the typalign?
  size = MAXALIGN(size);

  PaxColumn::Append(buffer, origin_size);
  while (data_->Available() < size) {
    data_->ReSize(data_->Capacity() * 2);
  }

  if (lengths_->Available() == 0) {
    lengths_->ReSize(lengths_->Capacity() * 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, origin_size);
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
  return PaxColumnTypeInMem::kTypeNonFixed;
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

size_t PaxNonFixedColumn::PhysicalSize() const { return estimated_size_; }

int64 PaxNonFixedColumn::GetOriginLength() const {
  return NO_ENCODE_ORIGIN_LEN;
}

int32 PaxNonFixedColumn::GetTypeLength() const { return -1; }

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);

  return std::make_pair(data_->GetBuffer() + offsets_[position],
                        (*lengths_)[position]);
}

std::pair<char *, size_t> PaxNonFixedColumn::GetRangeBuffer(size_t start_pos,
                                                            size_t len) {
  CBDB_CHECK((start_pos + len) <= GetNonNullRows() && len > 0,
             cbdb::CException::ExType::kExTypeOutOfRange);
  size_t range_len = 0;

  for (size_t i = start_pos; i < start_pos + len; i++) {
    range_len += (*lengths_)[i];
  }

  if (GetNonNullRows() == 0) {
    Assert(range_len == 0);
    return std::make_pair(data_->GetBuffer(), 0);
  }

  Assert(start_pos < offsets_.size());
  return std::make_pair(data_->GetBuffer() + offsets_[start_pos], range_len);
}

bool PaxNonFixedColumn::IsMemTakeOver() const {
  Assert(data_->IsMemTakeOver() == lengths_->IsMemTakeOver());
  return data_->IsMemTakeOver();
}

void PaxNonFixedColumn::SetMemTakeOver(bool take_over) {
  data_->SetMemTakeOver(take_over);
  lengths_->SetMemTakeOver(take_over);
}

};  // namespace pax
