#include "storage/column_projection_info.h"

namespace pax {
ColumnProjectionInfo::ColumnProjectionInfo(int natts, bool *proj)
  : natts_(natts), proj_(proj) {
  proj_atts_ = new int[natts];
  proj_atts_num_ = 0;
  BuildupProjectionInfo();
}

ColumnProjectionInfo::~ColumnProjectionInfo() {
  // Note proj_ is initliazed in BeginScanExtractColumns, which carries every single column projection info.
  // It should be freed in ColumnProjectionInfo deconstructer on EndScan phase.
  if (proj_)
    delete [] proj_;
  if (proj_atts_)
    delete [] proj_atts_;
}

// Initialization of all column projection information which are necessary for pax column filtering.
void ColumnProjectionInfo::BuildupProjectionInfo() {
  int num_proj_atts = 0;
  for (int index = 0; index < natts_; index++) {
    if (proj_[index]) {
      proj_atts_[num_proj_atts] = index;
      num_proj_atts++;
    }
  }
  proj_atts_num_ = num_proj_atts;
}
}  // namespace pax

