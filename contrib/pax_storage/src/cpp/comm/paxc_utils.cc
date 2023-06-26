#include "comm/paxc_utils.h"
#include "comm/cbdb_wrappers.h"

namespace paxc {
// Supress compiler warning usage for all PAX functions with unused input paramters.
void Unused(const void *args, ...) {
  va_list var_list;
  if (args == NULL) return;
  va_start(var_list, args);
  (void)var_list;
  va_end(var_list);
}
}  // namespace paxc

