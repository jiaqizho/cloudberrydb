#pragma once
#include <utility>
#include <vector>
#include <map>

#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"

namespace pax {
// PaxColumns are similar to the kind_struct in orc
// It is designed to be nested and some interfaces have semantic differences
// Inheriting PaxCommColumn use to be able to nest itself
class PaxColumns : public PaxColumn {
 public:
  explicit PaxColumns(const std::vector<orc::proto::Type_Kind> &types);

  PaxColumns();

  ~PaxColumns() override;

  void Clear() override;

  PaxColumn *operator[](uint64 i);

  void Append(PaxColumn *column);

  void Append(char *buffer, size_t size) override;

  void Set(DataBuffer<char> *data);

  size_t EstimatedSize() const override;

  // Get number of column in columns
  virtual size_t GetColumns() const;

  // Get the combine buffer of all columns
  std::pair<char *, size_t> GetBuffer() override;

  // Get the combine buffer of single column
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  size_t GetNonNullRows() const override;

  using PreCalcBufferFunc =
      std::function<void(const orc::proto::Stream_Kind &, size_t, size_t)>;

  // Get the combined data buffer of all columns
  // TODO(jiaqizho): consider add a new api which support split IO from
  // different column
  virtual DataBuffer<char> *GetDataBuffer(const PreCalcBufferFunc &func);

  inline void AddRows(size_t row_num) { row_nums_ += row_num; }
  inline size_t GetRows() override { return row_nums_; }

 protected:
  virtual size_t MeasureDataBuffer(const PreCalcBufferFunc &func);

  virtual void CombineDataBuffer();

 protected:
  std::vector<PaxColumn *> columns_;
  DataBuffer<char> *data_;
  size_t row_nums_;
};

}  //  namespace pax
