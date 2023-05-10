#pragma once
#include <sstream>
#include <string>
namespace cbdb {
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

// being of a try block w/o explicit handler
#define CBDB_TRY                           \
  do {                                     \
    cbdb::CErrorHandler *err_hdl__ = NULL; \
    try {                                  \
// begin of a try block
#define CBDB_TRY_HDL(perrhdl)                 \
  do {                                        \
    cbdb::CErrorHandler *err_hdl__ = perrhdl; \
    try {                                     \
// begin of a catch block
#define CBDB_CATCH_EX(exc)                          \
  }                                                 \
  catch (cbdb::CException & exc) {                  \
    if (NULL != err_hdl__) err_hdl__->Process(exc); \
  }

// catch c++ exception and rethrow ERROR to C code
// only used by the outer c++ code called by C
#define CBDB_CATCH_DEFAULT() \
  catch (...) {              \
    PG_RETHROW()

// end of a catch block
#define CBDB_CATCH_END \
  }                    \
  }                    \
  while (0)

#define CBDB_MATCH_EX(ex, extype) (ex.EType() == extype)
