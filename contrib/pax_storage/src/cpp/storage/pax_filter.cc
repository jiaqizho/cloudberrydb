#include "storage/pax_filter.h"

namespace pax {
ColumnProjectionInfo* MicroPartitionFilter::GetProjectionInfo() {
  return projection_info_;
}

void MicroPartitionFilter::SetProjectionInfo(ColumnProjectionInfo *projection_info) {
  projection_info_ = projection_info;
}
}  // namespace pax
