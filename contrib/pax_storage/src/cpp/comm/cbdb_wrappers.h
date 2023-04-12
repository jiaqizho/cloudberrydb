#pragma once
#include <cstddef>
#include <string>

#include "exceptions/CException.h"

#ifndef PAX_INDEPENDENT_MODE

extern "C" {
#include "postgres.h"  // NOLINT
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
}

#else

#include "comm/local_warppers.h"

#endif

namespace cbdb {
//---------------------------------------------------------------------------
//  @class:
//      CAutoExceptionStack
//
//  @doc:
//      Auto object for saving and restoring exception stack
//
//---------------------------------------------------------------------------
class CAutoExceptionStack final {
 private:
  // address of the global exception stack value
  void **m_global_exception_stack;

  // address of the global error context stack value
  void **m_global_error_context_stack;

  // value of exception stack when object is created
  void *m_exception_stack;

  // value of error context stack when object is created
  void *m_error_context_stack;

 public:
  CAutoExceptionStack(const CAutoExceptionStack &) = delete;
  CAutoExceptionStack(CAutoExceptionStack &&) = delete;
  void *operator new(std::size_t count, ...) = delete;
  void *operator new[](std::size_t count, ...) = delete;

  // ctor
  CAutoExceptionStack(void **global_exception_stack,
                      void **global_error_context_stack)
      : m_global_exception_stack(global_exception_stack),
        m_global_error_context_stack(global_error_context_stack),
        m_exception_stack(*global_exception_stack),
        m_error_context_stack(*global_error_context_stack) {}

  // dtor
  ~CAutoExceptionStack() {
    *m_global_exception_stack = m_exception_stack;
    *m_global_error_context_stack = m_error_context_stack;
  }

  // set the exception stack to the given address
  void SetLocalJmp(void *local_jump) { *m_global_exception_stack = local_jump; }
};  // class CAutoExceptionStack

}  // namespace cbdb

#define CBDB_WRAP_START                                                       \
  sigjmp_buf local_sigjmp_buf;                                                \
  {                                                                           \
    CAutoExceptionStack aes(reinterpret_cast<void **>(&PG_exception_stack),   \
                            reinterpret_cast<void **>(&error_context_stack)); \
    if (0 == sigsetjmp(local_sigjmp_buf, 0)) {                                \
    aes.SetLocalJmp(&local_sigjmp_buf)

#define CBDB_WRAP_END                                   \
  }                                                     \
  else {                                                \
    CBDB_RAISE(cbdb::CException::ExType::ExTypeCError); \
  }                                                     \
  }

// override the default new/delete to use current memory context
extern void *operator new(std::size_t size);
extern void *operator new[](std::size_t size);
extern void operator delete(void *ptr);
extern void operator delete[](void *ptr);

// specify memory context for this allocation without switching memory context
extern void *operator new(std::size_t size, MemoryContext ctx);
extern void *operator new[](std::size_t size, MemoryContext ctx);

namespace cbdb {
HTAB *HashCreate(const char *tabname, long nelem, const HASHCTL *info,
                 int flags);
void *HashSearch(HTAB *hashp, const void *keyPtr, HASHACTION action,
                 bool *foundPtr);
MemoryContext AllocSetCtxCreate(MemoryContext parent, const char *name,
                                Size minContextSize, Size initBlockSize,
                                Size maxBlockSize);
void MemoryCtxRegisterResetCallback(MemoryContext context,
                                    MemoryContextCallback *cb);

Oid RelationGetRelationId(Relation rel);

void *PointerFromDatum(Datum d);
int64 Int64FromDatum(Datum d);
struct varlena *PgDeToastDatumPacked(struct varlena *datum);
}  // namespace cbdb