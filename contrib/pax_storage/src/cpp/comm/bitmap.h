#pragma once

#include "comm/cbdb_api.h"

#include <assert.h>

#include <cstddef>

#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {
extern const uint8 kNumBits[];
#define BM_WORD_BITS (sizeof(T) << 3)
// log2(BM_WORD_BITS)
#define BM_WORD_SHIFTS \
  (sizeof(T) == 1 ? 3 : (sizeof(T) == 2 ? 4 : (sizeof(T) == 4 ? 5 : 6)))
#define BM_INDEX_WORD_OFF(index) ((index) >> BM_WORD_SHIFTS)
#define BM_INDEX_BIT_OFF(index) ((index) & (BM_WORD_BITS - 1))
#define BM_INDEX_BIT(index) (1ULL << BM_INDEX_BIT_OFF(index))
template <typename T>
struct BitmapRaw final {
 public:
  inline void Set(uint32 index) {
    bitmap[BM_INDEX_WORD_OFF(index)] |= BM_INDEX_BIT(index);
  }
  // set first the bits [0, index] to 1
  inline void SetN(uint32 index) {
    memset(&bitmap[0], -1, sizeof(T) * BM_INDEX_WORD_OFF(index));
    bitmap[BM_INDEX_WORD_OFF(index)] |= (BM_INDEX_BIT(index) << 1) - 1;
  }
  inline void Clear(uint32 index) {
    bitmap[BM_INDEX_WORD_OFF(index)] &= ~BM_INDEX_BIT(index);
  }
  inline void ClearN(uint32 index) {
    memset(&bitmap[0], 0, sizeof(T) * BM_INDEX_WORD_OFF(index));
    bitmap[BM_INDEX_WORD_OFF(index)] &= ~((BM_INDEX_BIT(index) << 1) - 1);
  }
  inline void ClearAll() {
    AssertImply(size > 0, bitmap);
    if (size > 0) memset(&bitmap[0], 0, sizeof(T) * size);
  }
  inline bool Test(uint32 index) const {
    return (bitmap[BM_INDEX_WORD_OFF(index)] & BM_INDEX_BIT(index)) != 0;
  }
  // invert the bit and return the old value.
  inline bool Toggle(uint32 index) {
    return !((bitmap[BM_INDEX_WORD_OFF(index)] ^= BM_INDEX_BIT(index)) &
             BM_INDEX_BIT(index));
  }
  inline size_t WordBits(T v) const {
    if (sizeof(T) == 1)
      return kNumBits[v];
    else if (sizeof(T) == 2)
      return kNumBits[v & 0xff] + kNumBits[v >> 8];
    else if (sizeof(T) == 4)
      return kNumBits[v & 0xff] + kNumBits[(v >> 8) & 0xff] +
             kNumBits[(v >> 16) & 0xff] + kNumBits[(v >> 24) & 0xff];
    else if (sizeof(T) == 8)
      return kNumBits[v & 0xff] + kNumBits[(v >> 8) & 0xff] +
             kNumBits[(v >> 16) & 0xff] + kNumBits[(v >> 24) & 0xff] +
             kNumBits[(v >> 32) & 0xff] + kNumBits[(v >> 40) & 0xff] +
             kNumBits[(v >> 48) & 0xff] + kNumBits[(v >> 56) & 0xff];
    return 0;
  }
  // count bits in range [0, index]
  inline size_t CountBits(uint32 index) const {
    size_t nbits = 0;
    for (uint32 i = 0; i < BM_INDEX_WORD_OFF(index); i++)
      nbits += WordBits(bitmap[i]);
    {
      auto w = bitmap[BM_INDEX_WORD_OFF(index)];
      nbits += WordBits(w & ((BM_INDEX_BIT(index) << 1) - 1));
    }

    return nbits;
  }
  // count bits in range [start, end]
  inline size_t CountBits(uint32 start_index, uint32 end_index) const {
    size_t nbits = 0;
    uint32 word_off = BM_INDEX_WORD_OFF(start_index);

    Assert(start_index <= end_index);

    if (BM_INDEX_WORD_OFF(end_index) == word_off) {
      T w = bitmap[word_off] >> BM_INDEX_BIT_OFF(start_index);
      return WordBits(w & ((1ULL << (end_index - start_index + 1)) - 1));
    }
    {
      T w = bitmap[BM_INDEX_WORD_OFF(start_index)];
      nbits += WordBits(w >> BM_INDEX_BIT_OFF(start_index));
    }
    for (uint32 i = BM_INDEX_WORD_OFF(start_index + BM_WORD_BITS),
                n = BM_INDEX_WORD_OFF(end_index);
         i < n; i++)
      nbits += WordBits(bitmap[i]);
    {
      auto w = bitmap[BM_INDEX_WORD_OFF(end_index)];
      nbits += WordBits(w & ((BM_INDEX_BIT(end_index) << 1) - 1));
    }
    return nbits;
  }

  inline bool HasEnoughSpace(uint32 index) const {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                  sizeof(T) == 8);
    static_assert(BM_WORD_BITS == (1 << BM_WORD_SHIFTS));
    return (index >> BM_WORD_SHIFTS) < size;
  }
  inline bool Empty() const {
    if (!bitmap) return true;
    for (size_t i = 0; i < size; i++)
      if (bitmap[i]) return false;
    return true;
  }
  BitmapRaw() = default;
  BitmapRaw(T *buffer, size_t size) : bitmap(buffer), size(size) {}
  BitmapRaw(const BitmapRaw &) = delete;
  BitmapRaw(BitmapRaw &&raw) : bitmap(raw.bitmap), size(raw.size) {
    raw.bitmap = nullptr;
    raw.size = 0;
  }
  BitmapRaw &operator=(BitmapRaw) = delete;
  BitmapRaw &operator=(BitmapRaw &) = delete;
  BitmapRaw &operator=(const BitmapRaw &) = delete;
  BitmapRaw &operator=(BitmapRaw &&raw) {
    if (this != &raw) {
      PAX_DELETE_ARRAY(bitmap);
      bitmap = raw.bitmap;
      size = raw.size;
      raw.bitmap = nullptr;
      raw.size = 0;
    }
    return *this;
  }

  ~BitmapRaw() = default;

  T *bitmap = nullptr;
  size_t size = 0;
};

template <typename T>
class BitmapTpl final {
 public:
  using BitmapMemoryPolicy = void (*)(BitmapRaw<T> &, uint32);
  explicit BitmapTpl(uint32 initial_size = 16,
                     BitmapMemoryPolicy policy = DefaultBitmapMemoryPolicy) {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                  sizeof(T) == 8);
    static_assert(BM_WORD_BITS == (1 << BM_WORD_SHIFTS));
    policy_ = policy;
    policy(raw_, Max(initial_size, 16));
  }
  explicit BitmapTpl(const BitmapRaw<T> &raw, BitmapMemoryPolicy policy) {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                  sizeof(T) == 8);
    static_assert(BM_WORD_BITS == (1 << BM_WORD_SHIFTS));
    Assert(policy == ReadOnlyRefBitmap || policy == ReadOnlyOwnBitmap);
    policy_ = policy;
    raw_.bitmap = raw.bitmap;
    raw_.size = raw.size;
  }
  BitmapTpl(const BitmapTpl &tpl) = delete;
  BitmapTpl(BitmapTpl &&tpl)
      : raw_(std::move(tpl.raw_)), policy_(tpl.policy_) {}
  BitmapTpl &operator=(const BitmapTpl &tpl) = delete;
  BitmapTpl &operator=(BitmapTpl &&tpl) = delete;
  ~BitmapTpl() {
    // Reference doesn't free the memory
    if (policy_ == ReadOnlyRefBitmap) raw_.bitmap = nullptr;
  }

  BitmapTpl *Clone() const {
    auto p = PAX_NEW_ARRAY<T>(raw_.size);
    memcpy(p, raw_.bitmap, sizeof(T) * raw_.size);
    BitmapRaw<T> bm_raw(p, raw_.size);
    return PAX_NEW<BitmapTpl>(std::move(bm_raw));
  }

  inline size_t WordBits() const { return BM_WORD_BITS; }
  inline void Set(uint32 index) {
    if (unlikely(!raw_.HasEnoughSpace(index))) policy_(raw_, index);
    raw_.Set(index);
  }
  inline void SetN(uint32 index) {
    if (unlikely(!raw_.HasEnoughSpace(index))) policy_(raw_, index);
    raw_.SetN(index);
  }
  inline void Clear(uint32 index) {
    if (likely(raw_.HasEnoughSpace(index))) raw_.Clear(index);
  }
  inline void ClearN(uint32 index) {
    if (raw_.HasEnoughSpace(index))
      raw_.ClearN(index);
    else
      raw_.ClearAll();
  }
  inline void ClearAll() { raw_.ClearAll(); }
  inline bool Test(uint32 index) const {
    if (likely(raw_.HasEnoughSpace(index))) return raw_.Test(index);
    return false;
  }
  // invert the bit and return the old value.
  inline bool Toggle(uint32 index) {
    if (unlikely(!raw_.HasEnoughSpace(index))) policy_(raw_, index);
    return raw_.Toggle(index);
  }
  // count bits in range [0, index]
  inline size_t CountBits(uint32 index) const {
    if (raw_.size == 0) return 0;
    if ((raw_.size << BM_WORD_SHIFTS) <= index)
      index = (raw_.size << BM_WORD_SHIFTS) - 1;
    return raw_.CountBits(index);
  }
  inline size_t CountBits(uint32 start_index, uint32 end_index) const {
    if ((raw_.size << BM_WORD_SHIFTS) <= start_index) return 0;
    if ((raw_.size << BM_WORD_SHIFTS) <= end_index)
      end_index = (raw_.size << BM_WORD_SHIFTS) - 1;
    Assert(start_index <= end_index);
    return raw_.CountBits(start_index, end_index);
  }

  inline bool Empty() const { return raw_.Empty(); }

  BitmapMemoryPolicy Policy() const { return policy_; }

  const BitmapRaw<T> &Raw() const { return raw_; }
  BitmapRaw<T> &Raw() { return raw_; }

  static void DefaultBitmapMemoryPolicy(BitmapRaw<T> &raw, uint32 index) {
    auto old_bitmap = raw.bitmap;
    auto old_size = raw.size;
    auto size = Max(BM_INDEX_WORD_OFF(index) + 1, old_size * 2);
    auto p = PAX_NEW_ARRAY<T>(size);
    if (old_size > 0) memcpy(p, old_bitmap, sizeof(T) * old_size);
    memset(&p[old_size], 0, sizeof(T) * (size - old_size));
    raw.bitmap = p;
    raw.size = size;
    PAX_DELETE_ARRAY(old_bitmap);
  }
  static void ReadOnlyRefBitmap(BitmapRaw<T> & /*raw*/, uint32 /*index*/) {
    // raise
    CBDB_RAISE(cbdb::CException::kExTypeInvalidMemoryOperation);
  }
  static void ReadOnlyOwnBitmap(BitmapRaw<T> & /*raw*/, uint32 /*index*/) {
    CBDB_RAISE(cbdb::CException::kExTypeInvalidMemoryOperation);
  }

  static inline size_t RequireWords(size_t nbits) {
    return nbits ? ((nbits - 1) >> BM_WORD_SHIFTS) + 1 : 0;
  }
  inline size_t CurrentBytes() const { return sizeof(T) * raw_.size; }
  inline size_t MinimalStoredBytes(size_t nbits) {
    auto nwords = RequireWords(nbits);
    if (nwords > raw_.size) nwords = raw_.size;
    while (nwords > 0 && raw_.bitmap[nwords - 1] == 0) nwords--;
    return nwords * sizeof(T);
  }

  static BitmapTpl<T> *BitmapTplCopy(const BitmapTpl<T> *bitmap) {
    if (bitmap == nullptr) return nullptr;
    return bitmap->Clone();
  }

  static BitmapTpl<T> *Union(const BitmapTpl<T> *a, const BitmapTpl<T> *b) {
    BitmapTpl<T> *result;
    const BitmapTpl<T> *large;
    const BitmapTpl<T> *small;

    if (a == nullptr) return BitmapTplCopy(b);
    if (b == nullptr) return BitmapTplCopy(a);

    if (a->raw_.size < b->raw_.size) {
      large = b;
      small = a;
    } else {
      large = a;
      small = b;
    }

    result = BitmapTplCopy(large);
    for (size_t i = 0; i < small->raw_.size; i++)
      result->raw_.bitmap[i] |= small->raw_.bitmap[i];

    return result;
  }

 private:
  template <typename NewT, typename... Args>
  friend NewT *PAX_NEW(Args &&...args);
  inline bool HasEnoughSpace(uint32 index) const {
    return raw_.HasEnoughSpace(index);
  }
  BitmapTpl(BitmapRaw<T> &&raw)
      : raw_(std::move(raw)), policy_(DefaultBitmapMemoryPolicy) {}
  BitmapRaw<T> raw_;
  BitmapMemoryPolicy policy_;
};

using Bitmap8 = BitmapTpl<uint8>;
using Bitmap64 = BitmapTpl<uint64>;

}  // namespace pax
