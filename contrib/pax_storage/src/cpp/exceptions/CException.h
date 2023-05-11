#pragma once
#include <sstream>
#include <string>
namespace cbdb {
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

  static void Raise(CException ex) __attribute__((__noreturn__));
  static void Raise(ExType extype) __attribute__((__noreturn__));
  static void Raise(const char *filename, int line, ExType extype)
      __attribute__((__noreturn__));
  static void Reraise(CException ex) __attribute__((__noreturn__));
  static const char *ExTypeString(ExType extype);

 private:
  static const char *exception_names[];
  const char *m_filename;
  int m_lineno;
  ExType m_extype;
};

//---------------------------------------------------------------------------
//    @class:
//        CErrorHandler
//
//    @doc:
//        Error handler to be installed inside a worker;
//
//---------------------------------------------------------------------------
class CErrorHandler {
 public:
  CErrorHandler(const CErrorHandler &) = delete;

  // ctor
  CErrorHandler() = default;

  // dtor
  virtual ~CErrorHandler() = default;

  // process error
  virtual void Process(CException exception) = 0;
};  // class CErrorHandler

}  // namespace cbdb

#define CBDB_RAISE(...) cbdb::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)
#define CBDB_CHECK(check, ...) \
  if (!(check)) {              \
    CBDB_RAISE(__VA_ARGS__);   \
  }
