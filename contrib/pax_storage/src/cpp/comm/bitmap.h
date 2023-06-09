#pragma once

#include <assert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace pax {

class Bitmap {
 public:
  virtual ~Bitmap() {}
  virtual void Set(uint32_t index) = 0;
  virtual bool Test(uint32_t index) const = 0;
  virtual void Clear(uint32_t index) = 0;
  virtual void Reset() = 0;
  virtual bool BitmapFindFirst(uint32_t offset, bool value,
                               uint32_t *idx) const = 0;
  virtual uint32_t NumBits() const = 0;
};

class DynamicBitmap : public Bitmap {
 public:
  friend class BitmapIterator;
  DynamicBitmap() { bitmap_.resize(1024); }
  explicit DynamicBitmap(uint32_t size) { bitmap_.resize(size); }

  virtual ~DynamicBitmap() { bitmap_.clear(); }

  void Set(uint32_t index) override {
    if (index < 0 || index >= bitmap_.size()) {
      throw std::out_of_range("index out of range");
    }
    bitmap_[index] = true;
  }

  bool Test(uint32_t index) const override {
    if (index < 0 || index >= bitmap_.size()) {
      throw std::out_of_range("index out of range");
    }
    return bitmap_[index];
  }

  void Clear(uint32_t index) override {
    if (index < 0 || index >= bitmap_.size()) {
      throw std::out_of_range("index out of range");
    }
    bitmap_[index] = false;
  }

  void Reset() override { bitmap_.clear(); }

  void Resize(int size) { bitmap_.resize(size); }

  // TODO(gongxun): need to do optimization for this function
  bool BitmapFindFirst(uint32_t offset, bool value,
                       uint32_t *idx) const override {
    auto it = std::find(bitmap_.begin() + offset, bitmap_.end(), value);
    if (it == bitmap_.end()) {
      return false;
    }
    *idx = it - bitmap_.begin();
    return true;
  }

  uint32_t NumBits() const override { return bitmap_.size(); }

 private:
  std::vector<bool> bitmap_;
};

class FixedBitmap : public Bitmap {
 public:
  friend class BitmapIterator;
  explicit FixedBitmap(uint32_t size) {
    byte_size_ = (size >> 3) + (size & 7 ? 1 : 0);
    bitmap_ = new uint8_t[byte_size_];

    num_bits_ = size;
    memset(bitmap_, 0, byte_size_);
  }
  virtual ~FixedBitmap() { delete[] bitmap_; }

  void Set(uint32_t index) override {
    if (index < 0 || index >= num_bits_) {
      throw std::out_of_range("index out of range");
    }
    bitmap_[index >> 3] |= 1 << (index & 7);
  }

  bool Test(uint32_t index) const override {
    if (index < 0 || index >= num_bits_) {
      throw std::out_of_range("index out of range");
    }
    return bitmap_[index >> 3] & (1 << (index & 7));
  }

  void Reset() override { std::memset(bitmap_, 0, byte_size_); }

  void Clear(uint32_t index) override {
    if (index < 0 || index >= num_bits_) {
      throw std::out_of_range("index out of range");
    }
    bitmap_[index >> 3] &= ~(1 << (index & 7));
  }

  uint32_t Size() const { return byte_size_; }
  uint32_t NumBits() const override { return num_bits_; }
  bool BitmapFindFirst(uint32_t offset, bool value, uint32_t *idx) const {
    const uint64_t pattern64[2] = {0xffffffffffffffff, 0x0000000000000000};
    const uint8_t pattern8[2] = {0xff, 0x00};
    uint32_t bit;

    if (offset >= num_bits_) {
      return false;
    }

    // Jump to the byte at specified offset
    const uint8_t *p = bitmap_ + (offset >> 3);
    uint32_t num_bits = num_bits_ - offset;

    // Find a 'value' bit at the end of the first byte
    if ((bit = offset & 0x7)) {
      for (; bit < 8 && num_bits > 0; ++bit) {
        if (Test(((p - bitmap_) << 3) + bit) == value) {
          *idx = ((p - bitmap_) << 3) + bit;
          return true;
        }

        num_bits--;
      }
      p++;
    }

    // check 64bit at the time for a 'value' bit
    const uint64_t *u64 = (const uint64_t *)p;
    while (num_bits >= 64 && *u64 == pattern64[value]) {
      num_bits -= 64;
      u64++;
    }

    // check 8bit at the time for a 'value' bit
    p = (const uint8_t *)u64;
    while (num_bits >= 8 && *p == pattern8[value]) {
      num_bits -= 8;
      p++;
    }

    // Find a 'value' bit at the beginning of the last byte
    for (bit = 0; num_bits > 0; ++bit) {
      if (Test(((p - bitmap_) << 3) + bit) == value) {
        *idx = ((p - bitmap_) << 3) + bit;
        return true;
      }
      num_bits--;
    }

    return false;
  }

 private:
  FixedBitmap(const FixedBitmap &other) = delete;
  FixedBitmap(FixedBitmap &&other) = delete;
  FixedBitmap &operator=(const FixedBitmap &other) = delete;
  FixedBitmap &operator=(FixedBitmap &&other) = delete;

  uint32_t byte_size_;
  uint32_t num_bits_;
  uint8_t *bitmap_;
};

class BitmapIterator {
 public:
  explicit BitmapIterator(Bitmap *map) : offset_(0), bitmap_(map) {}

  void SeekTo(size_t bit) {
    assert(bit < bitmap_->NumBits());
    offset_ = bit;
  }

  int32_t Next(bool value) {
    int32_t len = bitmap_->NumBits() - offset_;
    if (len <= 0) return -1;
    uint32_t index;
    if (bitmap_->BitmapFindFirst(offset_, value, &index)) {
      offset_ = index + 1;
      return index;
    }
    return -1;
  }

 private:
  uint32_t offset_;
  Bitmap *bitmap_;
};
}  // namespace pax
