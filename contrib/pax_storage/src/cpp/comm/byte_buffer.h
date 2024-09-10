#pragma once
#include <memory>

namespace pax {
class ByteBuffer {
 public:
  ByteBuffer(size_t capacity, size_t size);
  ByteBuffer(size_t capacity);
  ByteBuffer();

  ByteBuffer(const ByteBuffer &) = delete;
  ByteBuffer(ByteBuffer &&other);
  ByteBuffer &operator=(const ByteBuffer &) = delete;
  ByteBuffer &operator=(ByteBuffer &&tmp);
  ~ByteBuffer();

  const void *Addr() const { return buffer_; }
  void *Addr() { return buffer_; }
  size_t Size() const { return size_; }
  void SetSize(size_t size) {
    EnsureBufferSize(size);
    size_ = size;
  }
  bool Empty() const { return size_ == 0; }
  operator bool() const { return size_ > 0; }

  size_t Capacity() const { return capacity_; }

  void EnsureBufferSize(size_t size) {
    if (capacity_ >= size) return;
    EnsureBufferSizeInternal(size);
  }
 private:
  void InitializeBuffer();
  void EnsureBufferSizeInternal(size_t size);
  void *buffer_; // owned by me
  size_t size_;
  size_t capacity_;
};

}