#include "CException.h"

namespace cbdb {

void CException::Raise(CException ex) { throw ex; }

void CException::Raise(ExType extype) { Raise(CException(extype)); }

void CException::Raise(const char *filename, int lineno, ExType extype) {
  Raise(CException(filename, lineno, extype));
}

void CException::Reraise(CException ex) { Raise(ex); }

const char *CException::exception_names[] = {"Invalid ExType",
                                             "Assert Failure",
                                             "Abort",
                                             "Out of Memory",
                                             "IO Error",
                                             "C ERROR",
                                             "Logic ERROR",
                                             "Invalid memory operation",
                                             "Schema not match",
                                             "Invalid orc format",
                                             "Out of range"};

const char *CException::ExTypeString(ExType extype) {
  int n = static_cast<int>((sizeof(exception_names) / sizeof(char *)));
  int i = static_cast<int>(extype);
  if (i < 0 || i >= n) i = 0;
  return exception_names[i];
}

}  // namespace cbdb
