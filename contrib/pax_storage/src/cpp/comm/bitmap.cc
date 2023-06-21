#include "comm/bitmap.h"

#include "exceptions/CException.h"

namespace pax {

DynamicBitmap::DynamicBitmap() { bitmap_.resize(1024); }
DynamicBitmap::DynamicBitmap(uint32 size) { bitmap_.resize(size); }

DynamicBitmap::~DynamicBitmap() { bitmap_.clear(); }

void DynamicBitmap::Set(uint32 index) {
  CBDB_CHECK(index >= 0 && index < bitmap_.size(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  bitmap_[index] = true;
}

bool DynamicBitmap::Test(uint32 index) const {
  CBDB_CHECK(index >= 0 && index < bitmap_.size(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return bitmap_[index];
}

void DynamicBitmap::Clear(uint32 index) {
  CBDB_CHECK(index >= 0 && index < bitmap_.size(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  bitmap_[index] = false;
}

void DynamicBitmap::Reset() { bitmap_.clear(); }

void DynamicBitmap::Resize(int size) { bitmap_.resize(size); }

// TODO(gongxun): need to do optimization for this function
bool DynamicBitmap::BitmapFindFirst(uint32 offset, bool value,
                                    uint32 *idx) const {
  auto it = std::find(bitmap_.begin() + offset, bitmap_.end(), value);
  if (it == bitmap_.end()) {
    return false;
  }
  *idx = it - bitmap_.begin();
  return true;
}

uint32 DynamicBitmap::NumBits() const { return bitmap_.size(); }

FixedBitmap::FixedBitmap(uint32 size) {
  byte_size_ = (size >> 3) + (size & 7 ? 1 : 0);
  bitmap_ = new uint8[byte_size_];

  num_bits_ = size;
  memset(bitmap_, 0, byte_size_);
}

FixedBitmap::~FixedBitmap() { delete[] bitmap_; }

void FixedBitmap::Set(uint32 index) {
  CBDB_CHECK(index >= 0 && index < num_bits_,
             cbdb::CException::ExType::kExTypeOutOfRange);
  bitmap_[index >> 3] |= 1 << (index & 7);
}

bool FixedBitmap::Test(uint32 index) const {
  CBDB_CHECK(index >= 0 && index < num_bits_,
             cbdb::CException::ExType::kExTypeOutOfRange);
  return bitmap_[index >> 3] & (1 << (index & 7));
}

void FixedBitmap::Reset() { std::memset(bitmap_, 0, byte_size_); }

void FixedBitmap::Clear(uint32 index) {
  CBDB_CHECK(index >= 0 && index < num_bits_,
             cbdb::CException::ExType::kExTypeOutOfRange);
  bitmap_[index >> 3] &= ~(1 << (index & 7));
}

uint32 FixedBitmap::Size() const { return byte_size_; }
uint32 FixedBitmap::NumBits() const { return num_bits_; }
bool FixedBitmap::BitmapFindFirst(uint32 offset, bool value,
                                  uint32 *idx) const {
  const uint64 pattern64[2] = {0xffffffffffffffff, 0x0000000000000000};
  const uint8 pattern8[2] = {0xff, 0x00};
  uint32 bit;

  if (offset >= num_bits_) {
    return false;
  }

  // Jump to the byte at specified offset
  const uint8 *p = bitmap_ + (offset >> 3);
  uint32 num_bits = num_bits_ - offset;

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
  const uint64 *u64 = (const uint64 *)p;
  while (num_bits >= 64 && *u64 == pattern64[value]) {
    num_bits -= 64;
    u64++;
  }

  // check 8bit at the time for a 'value' bit
  p = (const uint8 *)u64;
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

BitmapIterator::BitmapIterator(Bitmap *map) : offset_(0), bitmap_(map) {}

void BitmapIterator::SeekTo(size_t bit) {
  Assert(bit < bitmap_->NumBits());
  offset_ = bit;
}

int32 BitmapIterator::Next(bool value) {
  int32 len = bitmap_->NumBits() - offset_;
  if (len <= 0) return -1;
  uint32 index;
  if (bitmap_->BitmapFindFirst(offset_, value, &index)) {
    offset_ = index + 1;
    return index;
  }
  return -1;
}

}  // namespace pax
