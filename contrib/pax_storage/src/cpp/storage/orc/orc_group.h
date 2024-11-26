#pragma once
#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition.h"

namespace pax {

namespace tools {
class OrcDumpReader;
}

class OrcGroup : public MicroPartitionReader::Group {
 public:
  OrcGroup(
      std::unique_ptr<PaxColumns> &&pax_column, size_t row_offset,
      const std::vector<int> *proj_col_index,
      std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap = nullptr);

  ~OrcGroup() override;

  size_t GetRows() const override;

  size_t GetRowOffset() const override;

  std::shared_ptr<PaxColumns> GetAllColumns() const override;

  std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(TupleDesc desc, size_t column_index,
                                        size_t row_index) override;
  void SetVisibilityMap(std::shared_ptr<Bitmap8> visibility_bitmap) override {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

  std::shared_ptr<Bitmap8> GetVisibilityMap() const override {
    return micro_partition_visibility_bitmap_;
  }

 protected:
  void CalcNullShuffle(const std::shared_ptr<PaxColumn> &column,
                       size_t column_index);

  // Used to get the no missing column
  std::pair<Datum, bool> GetColumnValueNoMissing(size_t column_index,
                                                 size_t row_index);

  // Used in `ReadTuple`
  // Different from the other `GetColumnValue` function, in this function, if a
  // null row is encountered, then we will perform an accumulation operation on
  // `null_counts`. If no null row is encountered, the offset of the row data
  // will be calculated through `null_counts`. The other `GetColumnValue`
  // function are less efficient in `foreach` because they have to calculate the
  // offset of the row data from scratch every time.
  virtual std::pair<Datum, bool> GetColumnValue(
      const std::shared_ptr<PaxColumn> &column, size_t row_index,
      uint32 *null_counts);

 protected:
  std::shared_ptr<PaxColumns> pax_columns_;
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_;
  size_t row_offset_;
  size_t current_row_index_;
  std::vector<std::shared_ptr<MemoryObject>> buffer_holders_;

 private:
  friend class tools::OrcDumpReader;
  std::vector<uint32> current_nulls_;
  std::vector<uint32 *> nulls_shuffle_;
  // only a reference, owner by pax_filter
  const std::vector<int> *proj_col_index_;
};

class OrcVecGroup final : public OrcGroup {
 public:
  OrcVecGroup(std::unique_ptr<PaxColumns> &&pax_column, size_t row_offset,
              const std::vector<int> *proj_col_index);

 private:
  std::pair<Datum, bool> GetColumnValue(
      const std::shared_ptr<PaxColumn> &column, size_t row_index,
      uint32 *null_counts) override;
};

}  // namespace pax
