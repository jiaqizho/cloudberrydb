#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxDeleter {
 public:
  explicit CPaxDeleter(Relation rel);

 private:
  Relation rel_;
};  // class CPaxDeleter
}  // namespace pax
