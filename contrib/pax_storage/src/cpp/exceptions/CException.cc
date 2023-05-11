#include "CException.h"

#include <cstdarg>
#include <cstring>
namespace cbdb {
ErrorMessage::ErrorMessage() {
  index_ = 0;
  message_[0] = '\0';
}
ErrorMessage::ErrorMessage(const ErrorMessage &message) {
  index_ = message.index_;
  std::memcpy(message_, message.message_, static_cast<size_t>(message.index_));
}
void ErrorMessage::Append(const char *format, ...) noexcept {
  unsigned index = (unsigned)index_;
  if (index < sizeof(message_)) {
    va_list ap;
    int n;
    va_start(ap, format);
    n = vsnprintf(&message_[index], sizeof(message_) - index, format, ap);
    va_end(ap);
    if (n > 0) index_ += n;
  }
}

void ErrorMessage::AppendV(const char *format, va_list ap) noexcept {
  unsigned index = (unsigned)index_;
  if (index < sizeof(message_)) {
    int n;
    n = vsnprintf(&message_[index], sizeof(message_) - index, format, ap);
    if (n > 0) index_ += n;
  }
}
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
