#pragma once

extern "C" {
#include "postgres.h"  // NOLINT
#include "access/relscan.h"
}

namespace pax {
class CPaxScanDesc {
 public:
  static TableScanDesc CreateTableScanDesc();
  ~CPaxScanDesc();

 private:
  CPaxScanDesc() {}

};  // class CPaxScanDesc

}  // namespace pax
