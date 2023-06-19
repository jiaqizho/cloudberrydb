#pragma once

#include "comm/cbdb_api.h"

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
  virtual void Set(uint32 index) = 0;
  virtual bool Test(uint32 index) const = 0;
  virtual void Clear(uint32 index) = 0;
  virtual void Reset() = 0;
  virtual bool BitmapFindFirst(uint32 offset, bool value,
                               uint32 *idx) const = 0;
  virtual uint32 NumBits() const = 0;
};

class DynamicBitmap : public Bitmap {
 public:
  friend class BitmapIterator;
  DynamicBitmap();
  explicit DynamicBitmap(uint32 size);

  virtual ~DynamicBitmap();

  void Set(uint32 index) override;

  bool Test(uint32 index) const override;

  void Clear(uint32 index) override;

  void Reset() override;

  void Resize(int size);

  // TODO(gongxun): need to do optimization for this function
  bool BitmapFindFirst(uint32 offset, bool value, uint32 *idx) const override;

  uint32 NumBits() const override;

 private:
  std::vector<bool> bitmap_;
};

class FixedBitmap : public Bitmap {
 public:
  friend class BitmapIterator;
  explicit FixedBitmap(uint32 size);

  virtual ~FixedBitmap();

  void Set(uint32 index) override;

  bool Test(uint32 index) const override;

  void Reset() override;

  void Clear(uint32 index) override;

  uint32 Size() const;

  uint32 NumBits() const override;

  bool BitmapFindFirst(uint32 offset, bool value, uint32 *idx) const;

 private:
  FixedBitmap(const FixedBitmap &other) = delete;
  FixedBitmap(FixedBitmap &&other) = delete;
  FixedBitmap &operator=(const FixedBitmap &other) = delete;
  FixedBitmap &operator=(FixedBitmap &&other) = delete;

  uint32 byte_size_;
  uint32 num_bits_;
  uint8 *bitmap_;
};

class BitmapIterator {
 public:
  explicit BitmapIterator(Bitmap *map);

  void SeekTo(size_t bit);

  int32 Next(bool value);

 private:
  uint32 offset_;
  Bitmap *bitmap_;
};
}  // namespace pax
