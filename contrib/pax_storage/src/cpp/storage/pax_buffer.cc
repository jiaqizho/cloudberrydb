#include "storage/pax_buffer.h"

#include "exceptions/CException.h"

namespace pax {
BlockBuffer::BlockBuffer(char *begin_offset, char *end_offset)
    : begin_offset_(begin_offset), end_offset_(end_offset) {}

BlockBufferBase::BlockBufferBase(char *ptr, size_t size, size_t offset)
    : block_pos_(ptr + offset), block_buffer_(ptr, ptr + size) {
  Assert(offset <= size);
}

void BlockBufferBase::Set(char *ptr, size_t size, size_t offset) {
  block_buffer_ = BlockBuffer(ptr, ptr + size);
  block_pos_ = ptr + offset;
}

void BlockBufferBase::Set(char *ptr, size_t size) {
  block_buffer_ = BlockBuffer(ptr, ptr + size);
  block_pos_ = ptr;
}

void BlockBufferBase::Write(char *ptr, size_t size) {
  Assert(Available() > size && Available() >= 0 && Available() < Capacity());
  Assert(Used() > 0 && Used() < Capacity());
  memcpy(block_pos_, ptr, size);
}

void BlockBufferBase::Combine(const BlockBufferBase &buffer) {
  Assert(Available() > buffer.Used());
  Write(buffer.block_buffer_.Start(), buffer.Used());
}

template <typename T>
DataBuffer<T>::DataBuffer(T *data_buffer, size_t size, bool allow_null,
                          bool mem_take_over)
    : BlockBufferBase(nullptr, 0, 0),
      mem_take_over_(mem_take_over),
      data_buffer_(data_buffer) {
  if (!allow_null && !data_buffer_ && size != 0) {
    data_buffer_ = reinterpret_cast<T *>(cbdb::Palloc(size));
  }
  BlockBufferBase::Set(reinterpret_cast<char *>(data_buffer_), size, 0);
}

template <typename T>  // NOLINT: redirect constructor
DataBuffer<T>::DataBuffer(size_t size)
    : DataBuffer(nullptr, size, false, true) {}

template <typename T>
T &DataBuffer<T>::operator[](size_t i) {
  return data_buffer_[i];
}

template <typename T>
size_t DataBuffer<T>::GetSize() {
  return Used() / sizeof(T);
}

template <typename T>
DataBuffer<T>::~DataBuffer() {
  if (mem_take_over_ && data_buffer_) {
    cbdb::Pfree(data_buffer_);
  }
}

template <typename T>
void DataBuffer<T>::Set(char *ptr, size_t size, size_t offset) {
  Assert(data_buffer_ == nullptr);
  BlockBufferBase::Set(ptr, size, offset);
  data_buffer_ = reinterpret_cast<T *>(ptr);
}

template <typename T>
void DataBuffer<T>::Set(char *ptr, size_t size) {
  Assert(data_buffer_ == nullptr);
  BlockBufferBase::Set(ptr, size);
  data_buffer_ = reinterpret_cast<T *>(ptr);
}

template <typename T>
void DataBuffer<T>::Write(T value) {
  Assert(Available() >= sizeof(T) && Available() <= Capacity());
  *(reinterpret_cast<T *>(block_pos_)) = value;
}

template <typename T>
void DataBuffer<T>::Write(T *ptr, size_t size) {
  Assert(Available() >= size && Available() <= Capacity());
  memcpy(block_pos_, reinterpret_cast<const char *>(ptr), size);
}

template <typename T>
void DataBuffer<T>::Write(const T *ptr, size_t size) {
  Assert(Available() >= size && Available() <= Capacity());
  memcpy(block_pos_, reinterpret_cast<const char *>(ptr), size);
}

template <typename T>
void DataBuffer<T>::Read(T *dst) {
  Assert(Used() > sizeof(T) && Used() <= Capacity());
  memcpy(dst, block_pos_, sizeof(T));
}

template <typename T>
void DataBuffer<T>::Read(void *dst, size_t n) {
  Assert(Used() > n && Used() <= Capacity());
  memcpy(dst, block_pos_, n);
}

template <typename T>
T *DataBuffer<T>::GetBuffer() const {
  return data_buffer_;
}

template <typename T>
T *DataBuffer<T>::GetAvailableBuffer() const {
  return data_buffer_ + Used();
}

template <typename T>
void DataBuffer<T>::ReSize(size_t size) {
  if (!mem_take_over_) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalidMemoryOperation);
  }

  size_t used = Used();
  if (data_buffer_) {
    data_buffer_ = reinterpret_cast<T *>(cbdb::RePalloc(data_buffer_, size));
  } else {
    data_buffer_ = reinterpret_cast<T *>(cbdb::Palloc(size));
  }
  BlockBufferBase::Set(reinterpret_cast<char *>(data_buffer_), size, used);
}

template <typename T>
bool DataBuffer<T>::IsMemTakeOver() const {
  return mem_take_over_;
}

template <typename T>
void DataBuffer<T>::SetMemTakeOver(bool take_over) {
  mem_take_over_ = take_over;
}

template <typename T>
void DataBuffer<T>::Clear() {
  if (mem_take_over_ && data_buffer_) {
    cbdb::Pfree(data_buffer_);
  }
  data_buffer_ = nullptr;
}

template class DataBuffer<char>;
template class DataBuffer<int16>;
template class DataBuffer<int32>;
template class DataBuffer<int64>;
template class DataBuffer<float>;
template class DataBuffer<double>;
template class DataBuffer<bool>;

}  // namespace pax
