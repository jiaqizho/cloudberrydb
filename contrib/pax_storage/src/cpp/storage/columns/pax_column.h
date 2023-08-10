#pragma once
#include <stddef.h>

#include <cstring>
#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

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

  // Empties the vector from all its elements, recursively.
  // Do not alter the current capacity.
  virtual void Clear();

  // Get column buffer from current column
  virtual std::pair<char *, size_t> GetBuffer() = 0;

  // Get buffer by position
  virtual std::pair<char *, size_t> GetBuffer(size_t position) = 0;

  // Get all rows number(contain null) from column
  virtual size_t GetRows();

  // Get rows number(not null) from column
  virtual size_t GetNonNullRows() const = 0;

  // Append new filed into current column
  virtual void Append(char *buffer, size_t size) = 0;

  // Contain null filed or not
  virtual bool HasNull();

  // Set null bitmap
  virtual void SetNulls(DataBuffer<bool> *null_bitmap);

  // Get null bitmaps
  DataBuffer<bool> *GetNulls() const;

  // Append a null filed into last position
  virtual void AppendNull();

  // Estimated memory size from current column
  virtual size_t EstimatedSize() const = 0;

  // Get current encoding type
  virtual ColumnEncoding_Kind GetEncodingType() const;

  // Get the data size without encoding/compress
  virtual int64 GetOriginLength() const = 0;

 protected:
  // null field bit map
  DataBuffer<bool> *null_bitmap_;

  // the column is encoded type
  ColumnEncoding_Kind encoded_type_;

  // whether the column is storage
  PaxColumnStorageType storage_type_;

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

  size_t GetNonNullRows() const override;

  void Clear() override;

  size_t EstimatedSize() const override;

  int64 GetOriginLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

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

  void Clear() override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t EstimatedSize() const override;

  int64 GetOriginLength() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

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
