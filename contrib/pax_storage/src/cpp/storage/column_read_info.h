#pragma once
#include "comm/cbdb_api.h"
#include <vector>
#include <string>
#include <utility>
#include <map>

namespace pax {
#define PAX_COLUMN_READ_INDEX_NOT_DEFINED (-1)

using PaxColumnReadInfo =  struct PaxColumnReadInfo {
  std::map<int, int> stream_read_columns;
  std::map<int, int> data_type_columns;
};

class ColumnReadInfo final {
 public:
  ColumnReadInfo() = default;
  ~ColumnReadInfo() = default;
  static std::vector<std::pair<int, int>> BuildupColumnReadInfo(const bool *proj, int column_number);
  static int GetReadColumnIndex(const int *proj_atts, int proj_atts_num, int index);

  // Check if there is full column read case:
  // 1. if no read columns option provided, then failback to full-column read in one shot.
  // 2. if read column options contains all table columns then just skip projection filtering for better IO performance.
  static inline bool CheckFullColumnsRead(std::vector<std::pair<int, int>> read_columns, size_t columns_number) {
    return(read_columns.empty() ||
              (read_columns.size() == 1 &&
              (read_columns[0].first == 0 && static_cast<size_t>(read_columns[0].second) == columns_number - 1)));
  }

  // Maintain Orc columns read with orc stream index mapping for data filtering case.
  static inline void SetReadColumnStreamIndex(std::map<int, int> &stream_read_columns, int index, int value) {  //NOLINT
    stream_read_columns[index] = value;
  }

  static inline int GetReadColumnStreamIndex(const std::map<int, int> &stream_read_columns, int index) {
    return stream_read_columns.at(index);
  }

  static inline void SetReadColumnDataTypeIndex(std::map<int, int> &data_type_columns, int index, int value) {  // NOLINT
    data_type_columns[index] = value;
  }

  // Get total actual read column number by column reading.
  static inline int GetReadColumnNum(const std::map<int, int> &data_type_columns) {
    return data_type_columns.size();
  }

  static inline int GetReadColumnDataTypeIndex(const std::map<int, int> &data_type_columns, int index) {
    auto iter = data_type_columns.find(index);
    return iter == data_type_columns.end() ? PAX_COLUMN_READ_INDEX_NOT_DEFINED : iter->second;
  }
};
}  //  namespace pax

