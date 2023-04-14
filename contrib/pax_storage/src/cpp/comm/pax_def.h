#pragma once

#include <vector>

#define Vector std::vector
// TBD define assert
#ifdef PAX_INDEPENDENT_MODE
#define Assert(...) \
  do {              \
  } while (0)
#endif
