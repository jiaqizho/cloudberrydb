#pragma once

#include "comm/cbdb_api.h"

#include <cstddef>
#include <string>

#include "exceptions/CException.h"
#include "storage/pax_block_id.h"

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
                      void **global_error_context_stack);

  // dtor
  ~CAutoExceptionStack();

  // set the exception stack to the given address
  void SetLocalJmp(void *local_jump);
};  // class CAutoExceptionStack

void *MemCtxAlloc(MemoryContext ctx, size_t size);
void *Palloc(size_t size);
void *Palloc0(size_t size);
void *RePalloc(void *ptr, size_t size);
void Pfree(void *ptr);

}  // namespace cbdb

// clang-format off
#define CBDB_WRAP_START                                           \
  sigjmp_buf local_sigjmp_buf;                                    \
  {                                                               \
      cbdb::CAutoExceptionStack aes(                              \
          reinterpret_cast<void **>(&PG_exception_stack),         \
          reinterpret_cast<void **>(&error_context_stack));       \
      if (0 == sigsetjmp(local_sigjmp_buf, 0))                    \
      {                                                           \
          aes.SetLocalJmp(&local_sigjmp_buf)

#define CBDB_WRAP_END                                             \
  }                                                               \
  else                                                            \
  {                                                               \
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCError);         \
  }                                                               \
  }
// clang-format on

// override the default new/delete to use current memory context
extern void *operator new(std::size_t size);
extern void *operator new[](std::size_t size);
extern void operator delete(void *ptr);
extern void operator delete[](void *ptr);

// specify memory context for this allocation without switching memory context
extern void *operator new(std::size_t size, MemoryContext ctx);
extern void *operator new[](std::size_t size, MemoryContext ctx);

namespace cbdb {
HTAB *HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                 int flags);
void *HashSearch(HTAB *hashp, const void *key_ptr, HASHACTION action,
                 bool *found_ptr);
MemoryContext AllocSetCtxCreate(MemoryContext parent, const char *name,
                                Size min_context_size, Size init_block_size,
                                Size max_block_size);
void MemoryCtxRegisterResetCallback(MemoryContext context,
                                    MemoryContextCallback *cb);

Oid RelationGetRelationId(Relation rel);

static inline void *PointerFromDatum(Datum d) noexcept {
  return DatumGetPointer(d);
}

static inline int8 DatumToInt8(Datum d) noexcept { return DatumGetInt8(d); }

static inline int16 DatumToInt16(Datum d) noexcept { return DatumGetInt16(d); }

static inline int32 DatumToInt32(Datum d) noexcept { return DatumGetInt32(d); }

static inline int64 DatumToInt64(Datum d) noexcept { return DatumGetInt64(d); }

static inline Datum Int8ToDatum(int8 d) noexcept { return Int8GetDatum(d); }

static inline int16 Int16ToDatum(int16 d) noexcept { return Int16GetDatum(d); }

static inline int32 Int32ToDatum(int32 d) noexcept { return Int32GetDatum(d); }

static inline int64 Int64ToDatum(int64 d) noexcept { return Int64GetDatum(d); }

void *PointerAndLenFromDatum(Datum d, int *len);

Datum DatumFromCString(const char *src, const size_t length);

Datum DatumFromPointer(const void *p, int16 typlen);

struct varlena *PgDeToastDatumPacked(struct varlena *datum);

// pax ctid mapping functions
void InitCommandResource();
void ReleaseCommandResource();
void GetTableIndexAndTableNumber(Oid table_rel_oid, uint8 *table_no,
                                 uint32 *table_index);
uint32 GetBlockNumber(Oid table_rel_oid, uint32 table_index,
                      paxc::PaxBlockId block_id);
paxc::PaxBlockId GetBlockId(Oid table_rel_oid, uint8 table_no,
                            uint32 block_number);

}  // namespace cbdb
