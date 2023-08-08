#pragma once
#include "comm/cbdb_api.h"
#include <map>
#include <set>
#include <string>

namespace pax {

// ColumnProjectionInfo Pax column projection information
// proj_atts_num_: total projection column number
// proj_atts_: array contains relation mapping between projection index and Pax column index.
class ColumnProjectionInfo final {
 public:
  ColumnProjectionInfo(int natts, bool *proj);
  ~ColumnProjectionInfo();

  inline int GetProjectionNattsNum() const { return natts_; }
  inline bool* GetProjectionArray() const { return proj_; }
  inline int GetProjectionAttsNum() const { return proj_atts_num_; }
  inline int GetProjectionAtts(int attrs) const { return proj_atts_[attrs]; }
  inline int* GetProjectionAttsArray() const { return proj_atts_; }

  void BuildupProjectionInfo();

 private:
  const int natts_;
  bool *proj_;
  int proj_atts_num_;
  int *proj_atts_;
};
}  //  namespace pax

