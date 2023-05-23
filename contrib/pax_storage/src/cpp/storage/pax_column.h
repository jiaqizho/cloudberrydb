#pragma once
#include <stddef.h>

#include <cstring>
#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/orc/orc_proto.pb.h"
#include "storage/pax_buffer.h"

namespace pax {

#define DEFAULT_CAPACITY 2048

// Used to mapping pg_type
enum PaxColumnTypeInMem {
  TYPE_INVALID = 1,
  TYPE_FIXED = 2,
  TYPE_NON_FIXED = 3
};

class PaxColumn {
 public:
  PaxColumn();

  virtual ~PaxColumn() = default;

  // Get the column in memory type
  virtual PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  // Empties the vector from all its elements, recursively.
  // Do not alter the current capacity.
  virtual void Clear();

  // Get column buffer from current column
  virtual std::pair<char *, size_t> GetBuffer() = 0;

  // Get buffer by position
  virtual std::pair<char *, size_t> GetBuffer(size_t position) = 0;

  // Get rows number from column
  virtual size_t GetRows() const = 0;

  // Append new filed into current column
  virtual void Append(char *buffer, size_t size) = 0;

  // Contain null filed or not
  virtual bool HasNull();

 protected:
  // TODO(jiaqizho): support not null implements
  std::vector<bool> non_null_;
  // whether there are any null values
  bool has_nulls_;
  // whether the vector batch is encoded
  bool is_encoded_;

 private:
  PaxColumn(const PaxColumn &);
  PaxColumn &operator=(const PaxColumn &);
};

template <typename T>
class PaxCommColumn : public PaxColumn {
 public:
  explicit PaxCommColumn(uint64 capacity);

  virtual ~PaxCommColumn();

  PaxCommColumn();

  virtual void Set(DataBuffer<T> *data);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  size_t GetRows() const override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  void Clear() override;

  std::pair<char *, size_t> GetBuffer() override;
#ifndef RUN_GTEST
 protected:  // NOLINT
#endif
  DataBuffer<T> *GetDataBuffer();

 protected:
  virtual void ReSize(uint64 capacity);

 protected:
  uint64 capacity_;
  DataBuffer<T> *data_;
};

extern template class PaxCommColumn<char>;
extern template class PaxCommColumn<int32>;

class PaxNonFixedColumn : public PaxColumn {
 public:
  explicit PaxNonFixedColumn(uint64 capacity);

  PaxNonFixedColumn();

  ~PaxNonFixedColumn();

  void Set(DataBuffer<char> *data, DataBuffer<int64> *lengths);

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  void Clear() override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t GetRows() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  DataBuffer<int64> *GetLengthBuffer() const;

  bool IsMemTakeOver() const;

  void SetMemTakeOver(bool take_over);
#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

 protected:
  DataBuffer<char> *data_;

  // orc needs to serialize int64 array
  DataBuffer<int64> *lengths_;
  std::vector<uint64> offsets_;
  uint64 capacity_;
};

// PaxColumns are similar to the kind_struct in orc
// It is designed to be nested and some interfaces have semantic differences
// Inheriting PaxCommColumn use to be able to nest itself
class PaxColumns : public PaxColumn {
 public:
  explicit PaxColumns(std::vector<orc::proto::Type_Kind> types);

  PaxColumns();

  ~PaxColumns();

  void Clear() override;

  PaxColumn *operator[](uint64 i);

  void Append(PaxColumn *column);

  void Append(char *buffer, size_t size) override;

  void Set(DataBuffer<char> *data);

  size_t GetRows() const override;

  // Get number of column in columns
  virtual size_t GetColumns() const;

  // Get the combine buffer of all columns
  std::pair<char *, size_t> GetBuffer() override;

  // Get the combine buffer of single column
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  using PreCalcBufferFunc =
      std::function<void(const orc::proto::Stream_Kind &, size_t, size_t)>;

  // Get the combined data buffer of all columns
  // TODO(jiaqizho): consider add a new api which support split IO from
  // different column
  virtual DataBuffer<char> *GetDataBuffer(const PreCalcBufferFunc &func);

 protected:
  virtual size_t MeasureDataBuffer(const PreCalcBufferFunc &func);

  virtual void CombineDataBuffer();

 protected:
  std::vector<PaxColumn *> columns_;
  DataBuffer<char> *data_;
};

};  // namespace pax
