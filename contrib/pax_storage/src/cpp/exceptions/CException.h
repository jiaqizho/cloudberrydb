#pragma once
#include "comm/cbdb_api.h"

#include <sstream>
#include <string>
namespace cbdb {

#define DEFAULT_STACK_MAX_DEPTH 63
#define DEFAULT_STACK_MAX_SIZE (DEFAULT_STACK_MAX_DEPTH + 1) * PIPE_MAX_PAYLOAD
// error message buffer
class ErrorMessage final {
 public:
  ErrorMessage();
  ErrorMessage(const ErrorMessage &message);
  void Append(const char *format, ...) noexcept;
  void AppendV(const char *format, va_list ap) noexcept;
  const char *message() const noexcept { return &message_[0]; }
  int length() const noexcept { return index_; }

 private:
  int index_ = 0;
  char message_[128];
};

class CException {
 public:
  enum ExType {
    ExTypeInvalid = 0,
    ExTypeUnImplements,
    ExTypeAssert,
    ExTypeAbort,
    ExTypeOOM,
    ExTypeIOError,
    ExTypeCError,
    ExTypeLogicError,
    ExTypeInvalidMemoryOperation,
    ExTypeSchemaNotMatch,
    ExTypeInvalidORCFormat,
    ExTypeOutOfRange
  };

  explicit CException(ExType extype)
      : m_filename(nullptr), m_lineno(0), m_extype(extype) {}

  CException(const char *filename, int lineno, ExType extype)
      : m_filename(filename), m_lineno(lineno), m_extype(extype) {}

  const char *Filename() const { return m_filename; }

  int Lineno() const { return m_lineno; }

  ExType EType() const { return m_extype; }

  std::string What() const {
    std::ostringstream buffer;
    buffer << m_filename << ":" << m_lineno << " " << exception_names[m_extype];
    return buffer.str();
  }

  const char *Stack() const { return stack_; }

  static void Raise(const char *filename, int line, ExType extype)
      __attribute__((__noreturn__));
  static void Raise(CException ex, bool reraise) __attribute__((__noreturn__));
  static void ReRaise(CException ex) __attribute__((__noreturn__));

 private:
  char stack_[DEFAULT_STACK_MAX_SIZE];
  static const char *exception_names[];
  const char *m_filename;
  int m_lineno;
  ExType m_extype;
};

}  // namespace cbdb

#define CBDB_RAISE(...) cbdb::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)
#define CBDB_RERAISE(ex) cbdb::CException::ReRaise(ex)
#define CBDB_CHECK(check, ...) \
  if (!(check)) {              \
    CBDB_RAISE(__VA_ARGS__);   \
  }
