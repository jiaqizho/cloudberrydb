#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxAccess {
 public:
  static void PaxCreateAuxBlocks(const Relation relation);
};  // class CPaxAccess

}  // namespace pax
