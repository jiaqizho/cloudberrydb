#pragma once
#include "storage/column_projection_info.h"

namespace pax {
class PaxAbstractFilter {
 public:
  PaxAbstractFilter() = default;
  virtual ~PaxAbstractFilter() = default;
};

class MicroPartitionFilter final : public PaxAbstractFilter {
 public:
  MicroPartitionFilter() = default;
  ~MicroPartitionFilter() override = default;
  ColumnProjectionInfo* GetProjectionInfo();
  void SetProjectionInfo(ColumnProjectionInfo *projection_info);
 private:
  ColumnProjectionInfo *projection_info_ = nullptr;
};
}  // namespace pax

