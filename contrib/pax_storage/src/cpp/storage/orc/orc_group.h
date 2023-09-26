#pragma once
#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition.h"

namespace pax {

class OrcGroup final : public MicroPartitionReader::Group {
 public:
  OrcGroup(PaxColumns *pax_column, size_t row_offset);

  ~OrcGroup() override;

  size_t GetRows() const override;

  size_t GetRowOffset() const override;

  PaxColumns *GetAllColumns() const override;

  std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(size_t column_index,
                                        size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(PaxColumn *column,
                                        size_t row_index) override;

 private:
  // Used in `ReadTuple`
  // Different from the other `GetColumnValue` function, in this function, if a
  // null row is encountered, then we will perform an accumulation operation on
  // `null_counts`. If no null row is encountered, the offset of the row data
  // will be calculated through `null_counts`. The other `GetColumnValue`
  // function are less efficient in `foreach` because they have to calculate the
  // offset of the row data from scratch every time.
  std::pair<Datum, bool> GetColumnValue(PaxColumn *column, size_t row_index,
                                        uint32 *null_counts);

 private:
  PaxColumns *pax_columns_;
  size_t row_offset_;
  size_t current_row_index_;
  uint32 *current_nulls_ = nullptr;
};

}  // namespace pax