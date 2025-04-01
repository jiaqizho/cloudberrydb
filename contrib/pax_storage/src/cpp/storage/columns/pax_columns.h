#pragma once
#include <map>
#include <utility>
#include <vector>

#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"

namespace pax {
// PaxColumns are similar to the kind_struct in orc
// It is designed to be nested and some interfaces have semantic differences
// Inheriting PaxCommColumn use to be able to nest itself
class PaxColumns : public PaxColumn {
 public:
  PaxColumns();

  ~PaxColumns() override;

  // Use to merge other columns, the columns pass in will be delete
  // This method is horizontal merge, `Merge(PaxColumn *column)` is
  // vertical merge.
  // a horizontal merge example:
  // columns1: c1   null c3
  // columns2: null c2   null
  // after merge: c1 c2 c3
  void Merge(PaxColumns *columns);

  PaxColumn *operator[](uint64 i);

  void Append(PaxColumn *column);

  void Append(char *buffer, size_t size) override;

  void AppendToast(char *buffer, size_t size) override;

  void Set(DataBuffer<char> *data);

  void SetStorageFormat(PaxStorageFormat format);

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetLengthsOriginLength() const override;

  int32 GetTypeLength() const override;

  PaxStorageFormat GetStorageFormat() const override;

  // Get number of column in columns
  virtual size_t GetColumns() const;

  // Get number of toast in columns
  size_t ToastCounts() override;

  // Set the external toast buffer
  void SetExternalToastDataBuffer(DataBuffer<char> *external_toast_data,
                                  const std::vector<size_t> &column_sizes);

  // Get the external toast data buffer
  DataBuffer<char> *GetExternalToastDataBuffer() override;

  // verify the external toast buffer
  void VerifyAllExternalToasts(const std::vector<uint64> &ext_toast_lens);

  // Get the combine buffer of all columns
  std::pair<char *, size_t> GetBuffer() override;

  // Get the combine buffer of single column
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  using ColumnStreamsFunc = std::function<void(
      const pax::porc::proto::Stream_Kind &, size_t, size_t, size_t)>;

  using ColumnEncodingFunc = std::function<void(
      const ColumnEncoding_Kind &, const uint64, const int64,
      const ColumnEncoding_Kind &, const uint64, const int64)>;

  // Get the combined data buffer of all columns
  // TODO(jiaqizho): consider add a new api which support split IO from
  // different column
  virtual DataBuffer<char> *GetDataBuffer(
      const ColumnStreamsFunc &column_streams_func,
      const ColumnEncodingFunc &column_encoding_func);

  inline void AddRows(size_t row_num) { row_nums_ += row_num; }
  inline size_t GetRows() const override { return row_nums_; }

 protected:
  static size_t AlignSize(size_t buf_len, size_t len, size_t align_size);

  size_t MeasureOrcDataBuffer(const ColumnStreamsFunc &column_streams_func,
                              const ColumnEncodingFunc &column_encoding_func);

  size_t MeasureVecDataBuffer(const ColumnStreamsFunc &column_streams_func,
                              const ColumnEncodingFunc &column_encoding_func);

  void CombineOrcDataBuffer();
  void CombineVecDataBuffer();

 protected:
  std::vector<PaxColumn *> columns_;
  std::vector<DataBuffer<char> *> data_holder_;
  DataBuffer<char> *data_;
  size_t row_nums_;

  PaxStorageFormat storage_format_;
};

}  //  namespace pax
