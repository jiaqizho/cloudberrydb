#include "storage/column_read_info.h"
#include <map>

namespace pax {
// Initialization of column read information for pax column-read filtering functionality.
std::vector<std::pair<int, int>> ColumnReadInfo::BuildupColumnReadInfo(const bool *proj, int column_number) {
  std::vector<std::pair<int, int>> column_read_info;
  int mread_lb = 0;
  int mread_ub = 0;

  // If no projection information passed, then perform a full-column read.
  if (proj == nullptr) {
    column_read_info.emplace_back(std::make_pair(0, column_number - 1));
    return column_read_info;
  }

  for (int index = 0; index < column_number; index++) {
    // Skip merge reading process in case index less than current upbound.
    if (index < mread_ub)
      continue;
    // Buildup seqential column read info for column merge reading.
    // Sequential column range endup condition:
    // 1. No-projection column.
    // 2. Last column with projection column.
    if (proj[index]) {
      mread_lb = mread_ub = index;
      while (mread_ub < column_number && proj[mread_ub]) {
        mread_ub++;
      }
      column_read_info.emplace_back(std::make_pair(mread_lb, mread_ub - 1));
    }
  }
  return column_read_info;
}

int ColumnReadInfo::GetReadColumnIndex(const int *proj_atts, int proj_atts_num, int index) {
  for (int idx = 0; idx < proj_atts_num; idx++) {
    if (proj_atts[idx] == index)
      return idx;
  }
  return PAX_COLUMN_READ_INDEX_NOT_DEFINED;
}
}  //  namespace pax

