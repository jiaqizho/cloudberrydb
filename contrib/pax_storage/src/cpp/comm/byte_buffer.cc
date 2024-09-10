#include "comm/byte_buffer.h"

#include "comm/pax_memory.h"

namespace pax {
ByteBuffer::ByteBuffer(size_t capacity, size_t size)
  : buffer_(nullptr)
  , size_(size)
  , capacity_(capacity) {
    InitializeBuffer();
  }

ByteBuffer::ByteBuffer(size_t capacity)
  : buffer_(nullptr)
  , size_(0)
  , capacity_(capacity) {
    InitializeBuffer();
  }

ByteBuffer::ByteBuffer()
  : buffer_(nullptr)
  , size_(0)
  , capacity_(0) { }

ByteBuffer::ByteBuffer(ByteBuffer &&other)
  : buffer_(other.buffer_)
  , size_(other.size_)
  , capacity_(other.capacity_) {
    other.buffer_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
}

ByteBuffer::~ByteBuffer() {
  auto p = buffer_;
  buffer_ = nullptr;
  size_ = 0;
  capacity_ = 0;

  PAX_FREE(p);
}

ByteBuffer &ByteBuffer::operator=(ByteBuffer &&tmp) {
  if (this != &tmp) {
    PAX_FREE(buffer_);
    buffer_ = tmp.buffer_;
    size_ = tmp.size_;
    capacity_ = tmp.capacity_;

    tmp.buffer_ = nullptr;
    tmp.size_ = 0;
    tmp.capacity_ = 0;
  }

  return *this;
}

void ByteBuffer::InitializeBuffer() {
  if (size_ > capacity_)
    capacity_ = size_;

  if (capacity_ > 0) {
    buffer_ = PAX_ALLOC(capacity_);
  }
}

#define ALIGN_SIZE(size, pow2) (((size) + ((pow2) - 1)) & ~((pow2) - 1))

void ByteBuffer::EnsureBufferSizeInternal(size_t size) {
  void *p;

  size = ALIGN_SIZE(size, 16);
  if (buffer_) {
    p = PAX_REALLOC(buffer_, size);
  } else {
    p = PAX_ALLOC(size);
  }
  buffer_ = p;
  capacity_ = size;
}

}