#pragma once
#include <stddef.h>

#include <cstring>
#include <iostream>
#include <utility>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_def.h"

namespace pax {

struct BlockBuffer {
  BlockBuffer(char* begin_offset, char* end_offset);

  inline char* Start() const { return begin_offset_; }

  inline char* End() const { return end_offset_; }

  inline size_t Size() const { return size_t(end_offset_ - begin_offset_); }

  inline void Resize(size_t size) { end_offset_ = begin_offset_ + size; }

  inline bool IsEmpty() const { return Size() == 0; }

  inline size_t Stripe(size_t size) const {
    return size_t(end_offset_ - begin_offset_) / size;
  }

  inline void Swap(BlockBuffer& other) {
    std::swap(begin_offset_, other.begin_offset_);
    std::swap(end_offset_, other.end_offset_);
  }

 private:
  char* begin_offset_;
  char* end_offset_;
};

class BlockBufferBase {
 public:
  BlockBufferBase(char* ptr, size_t size, size_t offset)
      : block_pos_(ptr + offset), block_buffer_(ptr, ptr + size) {
    Assert(offset <= size);
  }

  inline BlockBuffer& Buffer() { return block_buffer_; }
  inline char* Position() { return block_pos_; }

  /* Should not call Brush inside BlockBuffer or DataBuffer */
  inline void Brush(size_t size) { block_pos_ += size; }
  inline void BrushAll() { block_pos_ = block_buffer_.End(); }

  inline void BrushBack(size_t size) {
    size_t new_offset = Used() - size;
    Assert(new_offset > 0);
    block_pos_ = block_buffer_.Start() + new_offset;
  }

  inline void BrushBackAll() { block_pos_ = block_buffer_.Start(); }

  inline size_t Used() const {
    return size_t(block_pos_ - block_buffer_.Start());
  }

  inline size_t Available() const {
    return size_t(block_buffer_.End() - block_pos_);
  }

  inline size_t Capacity() const {
    return size_t(block_buffer_.End() - block_buffer_.Start());
  }

  virtual void Set(char* ptr, size_t size, size_t offset);

  virtual void Set(char* ptr, size_t size);

  void Write(char* ptr, size_t size);

  void Combine(const BlockBufferBase& buffer);

  virtual ~BlockBufferBase() = default;

 protected:
  char* block_pos_;
  BlockBuffer block_buffer_;
};

// DataBuffer used to manage a chunk of memory buffer.
// It provides a series of methods for template access,
// the internal buffer(T* data_buffer_) are ordered and can be used as a
// array. The internal buffer have a working pointer(block_pos_) which
// distinguishes the used buffer and available buffer.
// Below is the internal buffer visualization
//
//        internal buffer
// ----------------------------------
// | used buffer  | available buffer|
// ----------------------------------
//                ↑
//         working pointer
template <typename T>
class DataBuffer : public BlockBufferBase {
 public:
  // `data_buffer` can be exist buffer or nullptr
  // `size means` size of current buffer
  // `allow_null` if true then will not used `size` to alloc new buffer,
  // otherwise DataBuffer will used `size` to alloc a new buffer.
  // `mem_take_over` if true the internal buffer which passed by `data_buffer`
  // or new alloced will be freed when `DataBuffer` been freed, otherwise the
  // internal buffer should be freed by caller also the method `ReSize` can't be
  // called if `mem_take_over` is false.
  DataBuffer(T* data_buffer, size_t size, bool allow_null = true,
             bool mem_take_over = true);

  // will alloc a size of buffer and memory will take over with DataBuffer
  explicit DataBuffer(size_t size);

  // Direct access elements of internal buffer
  T& operator[](size_t i);

  // Get size of elements of internal buffer
  size_t GetSize();

  ~DataBuffer();

  // Set a memory buffer, should make sure internal buffer is nullptr.
  // This method is split from the constructor.
  // Sometimes caller need prealloc a DataBuffer without internal buffer.
  virtual void Set(char* ptr, size_t size, size_t offset);

  virtual void Set(char* ptr, size_t size);

  // Direct write a element into available buffer
  // Should call `Brush` after write
  void Write(T* ptr, size_t size);

  void Write(const T* ptr, size_t size);

  // Read all to dst pointer
  void Read(T* dst);

  void Read(void* dst, size_t n);

  // Get the internal buffer pointer
  T* GetBuffer() const;

  // Get the available buffer pointer
  T* GetAvailableBuffer() const;

  // Resize the internal buffer, size should bigger than capacity of internal
  // buffer `mem_take_over` should be true
  void ReSize(size_t size);

  // Is current internal buffer take over by DataBuffer
  bool IsMemTakeOver() const;

  void SetMemTakeOver(bool take_over);

  // Clear up the DataBuffer
  // Caller should call `Set` to reuse current `DataBuffer` after call `Clear`
  void Clear();

 private:
  bool mem_take_over_;
  T* data_buffer_ = nullptr;
};
extern template class DataBuffer<char>;
extern template class DataBuffer<int16>;
extern template class DataBuffer<int32>;
extern template class DataBuffer<int64>;
extern template class DataBuffer<float>;
extern template class DataBuffer<double>;

};  // namespace pax
