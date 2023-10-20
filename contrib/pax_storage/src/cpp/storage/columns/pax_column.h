#pragma once
#include <stddef.h>

#include <cstring>
#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "comm/bitmap.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

#define DEFAULT_CAPACITY 2048
#define NO_ENCODE_ORIGIN_LEN (-1)

// Used to mapping pg_type
enum PaxColumnTypeInMem { kTypeInvalid = 1, kTypeFixed = 2, kTypeNonFixed = 3 };

enum PaxColumnStorageType {
  // default non-vec store
  // which split null field and null bitmap
  kTypeStorageNonVec,
  // vec storage format
  // spec the storage format
  kTypeStorageVec,
};

class PaxColumn {
 public:
  PaxColumn();

  virtual ~PaxColumn();

  virtual PaxColumn *SetColumnEncodeType(ColumnEncoding_Kind encoding_type);

  virtual PaxColumn *SetColumnStorageType(PaxColumnStorageType storage_type);

  // Get the column in memory type
  virtual PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  // Get column buffer from current column
  virtual std::pair<char *, size_t> GetBuffer() = 0;

  // Get buffer by position
  virtual std::pair<char *, size_t> GetBuffer(size_t position) = 0;

  // Get buffer by range [start_pos, start_pos + len)
  virtual std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                                   size_t len) = 0;

  // Get all rows number(contain null) from column
  virtual size_t GetRows();

  // Get rows number(not null) from column
  virtual size_t GetNonNullRows() const = 0;

  // Get all rows number(not null) from column by range [start_pos, start_pos +
  // len)
  virtual size_t GetRangeNonNullRows(size_t start_pos, size_t len);

  // Append new filed into current column
  virtual void Append(char *buffer, size_t size);

  // Append a null filed into last position
  virtual void AppendNull();

  // Estimated memory size from current column
  virtual size_t PhysicalSize() const = 0;

  // Get current encoding type
  virtual ColumnEncoding_Kind GetEncodingType() const;

  // Get the data size without encoding/compress
  virtual int64 GetOriginLength() const = 0;

  // Get the type length, if non-fixed, will return -1
  virtual int32 GetTypeLength() const = 0;

  // Contain null filed or not
  bool HasNull();

  // Are all values null?
  bool AllNull() const;

  // Set null bitmap
  void SetBitmap(Bitmap8 *null_bitmap);

  // Get Bitmap
  Bitmap8 *GetBitmap() { return null_bitmap_; }

  void SetRows(size_t total_rows);

  virtual size_t GetAlignSize() const;

  virtual void SetAlignSize(size_t align_size);

 protected:
  // null field bit map
  Bitmap8 *null_bitmap_;

  // Writer: write pointer
  // Reader: total rows
  uint32 total_rows_;

  // the column is encoded type
  ColumnEncoding_Kind encoded_type_;

  // whether the column is storage
  PaxColumnStorageType storage_type_;

  // data part align size.
  // This field only takes effect when current column is no encoding/compress.
  //
  // About `type_align` in `pg_type` what you need to know:
  // 1. address alignment: the datum which return need alignment with
  // `type_align`
  // 2. datum padding: the datum need padding with `type_align`
  //
  // The align logic in pax:
  // 1. address alignment:
  //    - write will make sure address alignment(data stream) in disk
  //    - `ReadTuple` with/without memcpy should get a alignment datum
  // 2. datum padding: deal it in column `Append`
  size_t type_align_size_;

 private:
  PaxColumn(const PaxColumn &);
  PaxColumn &operator=(const PaxColumn &);
};

template <typename T>
class PaxCommColumn : public PaxColumn {
 public:
  explicit PaxCommColumn(uint64 capacity);

  ~PaxCommColumn() override;

  PaxCommColumn();

  virtual void Set(DataBuffer<T> *data);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

  int32 GetTypeLength() const override;

 protected:
  virtual void ReSize(uint64 capacity);

 protected:
  uint64 capacity_;
  DataBuffer<T> *data_;
};

extern template class PaxCommColumn<char>;
extern template class PaxCommColumn<int8>;
extern template class PaxCommColumn<int16>;
extern template class PaxCommColumn<int32>;
extern template class PaxCommColumn<int64>;
extern template class PaxCommColumn<float>;
extern template class PaxCommColumn<double>;

class PaxNonFixedColumn : public PaxColumn {
 public:
  explicit PaxNonFixedColumn(uint64 capacity);

  PaxNonFixedColumn();

  ~PaxNonFixedColumn() override;

  virtual void Set(DataBuffer<char> *data, DataBuffer<int64> *lengths,
                   size_t total_size);

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int32 GetTypeLength() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  DataBuffer<int64> *GetLengthBuffer() const;

  bool IsMemTakeOver() const;

  void SetMemTakeOver(bool take_over);

 protected:
  size_t estimated_size_;
  DataBuffer<char> *data_;

  // orc needs to serialize int64 array
  DataBuffer<int64> *lengths_;
  std::vector<uint64> offsets_;
};

};  // namespace pax
