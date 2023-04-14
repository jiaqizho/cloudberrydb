#pragma once

extern "C" {
#include "utils/relcache.h"
}

namespace pax {
class CPaxDeleter {
 public:
  explicit CPaxDeleter(Relation rel);

 private:
  Relation rel_;
};  // class CPaxDeleter
}  // namespace pax
