#include "comm/cbdb_wrappers.h"

#include "storage/paxc_block_map_manager.h"
extern "C" {
const char *progname;
}

namespace cbdb {

CAutoExceptionStack::CAutoExceptionStack(void **global_exception_stack,
                                         void **global_error_context_stack)
    : m_global_exception_stack(global_exception_stack),
      m_global_error_context_stack(global_error_context_stack),
      m_exception_stack(*global_exception_stack),
      m_error_context_stack(*global_error_context_stack) {}

CAutoExceptionStack::~CAutoExceptionStack() {
  *m_global_exception_stack = m_exception_stack;
  *m_global_error_context_stack = m_error_context_stack;
}

// set the exception stack to the given address
void CAutoExceptionStack::SetLocalJmp(void *local_jump) {
  *m_global_exception_stack = local_jump;
}

void *MemCtxAlloc(MemoryContext ctx, size_t size) {
  CBDB_WRAP_START;
  {
    { return MemoryContextAlloc(ctx, (Size)size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc(size_t size) {
  CBDB_WRAP_START;
  {
#ifdef RUN_GTEST
    if (TopMemoryContext == nullptr) {
      MemoryContextInit();
    }
#endif
    { return palloc(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc0(size_t size) {
  CBDB_WRAP_START;
  {
#ifdef RUN_GTEST
    if (TopMemoryContext == nullptr) {
      MemoryContextInit();
    }
#endif
    { return palloc0(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *RePalloc(void *ptr, size_t size) {
  CBDB_WRAP_START;
  { return repalloc(ptr, size); }
  CBDB_WRAP_END;
  return nullptr;
}

void Pfree(void *ptr) {
#ifdef RUN_GTEST
  if (ptr == nullptr) {
    return;
  }
#endif
  CBDB_WRAP_START;
  { pfree(ptr); }
  CBDB_WRAP_END;
}

}  // namespace cbdb

void *operator new(std::size_t size) { return cbdb::Palloc(size); }

void *operator new[](std::size_t size) { return cbdb::Palloc(size); }

void *operator new(std::size_t size, MemoryContext ctx) {
  return cbdb::MemCtxAlloc(ctx, size);
}

void *operator new[](std::size_t size, MemoryContext ctx) {
  return cbdb::MemCtxAlloc(ctx, size);
}

void operator delete(void *ptr) { cbdb::Pfree(ptr); }

void operator delete[](void *ptr) { cbdb::Pfree(ptr); }

HTAB *cbdb::HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                       int flags) {
  CBDB_WRAP_START;
  { return hash_create(tabname, nelem, info, flags); }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::HashSearch(HTAB *hashp, const void *key_ptr, HASHACTION action,
                       bool *found_ptr) {
  CBDB_WRAP_START;
  { return hash_search(hashp, key_ptr, action, found_ptr); }
  CBDB_WRAP_END;
  return nullptr;
}

MemoryContext cbdb::AllocSetCtxCreate(MemoryContext parent, const char *name,
                                      Size min_context_size, Size init_block_size,
                                      Size max_block_size) {
  CBDB_WRAP_START;
  {
    return AllocSetContextCreateInternal(parent, name, min_context_size,
                                         init_block_size, max_block_size);
  }
  CBDB_WRAP_END;
  return nullptr;
}

void cbdb::MemoryCtxRegisterResetCallback(MemoryContext context,
                                          MemoryContextCallback *cb) {
  CBDB_WRAP_START;
  { MemoryContextRegisterResetCallback(context, cb); }
  CBDB_WRAP_END;
}

Oid cbdb::RelationGetRelationId(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetRelid(rel); }
  CBDB_WRAP_END;
}

#ifdef RUN_GTEST
Datum cbdb::DatumFromCString(const char *src, const size_t length) {
  CBDB_WRAP_START;
  {
    text *result = reinterpret_cast<text *>(palloc(length + VARHDRSZ));
    SET_VARSIZE(result, length + VARHDRSZ);
    memcpy(VARDATA(result), src, length);
    return PointerGetDatum(result);
  }
  CBDB_WRAP_END;
  return 0;
}

Datum cbdb::DatumFromPointer(const void *p, int16 typlen) {
  CBDB_WRAP_START;
  {
    char *resultptr;
    resultptr = reinterpret_cast<char *>(palloc(typlen));
    memcpy(resultptr, p, typlen);
    return PointerGetDatum(resultptr);
  }
  CBDB_WRAP_END;
  return 0;
}
#endif

struct varlena *cbdb::PgDeToastDatumPacked(struct varlena *datum) {
  CBDB_WRAP_START;
  { return pg_detoast_datum_packed(datum); }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::PointerAndLenFromDatum(Datum d, int *len) {
  struct varlena *vl = nullptr;
  CBDB_WRAP_START;
  {
    vl = (struct varlena *)DatumGetPointer(d);
    *len = VARSIZE_ANY(vl);
    return static_cast<void *>(vl);
  }
  CBDB_WRAP_END;
  return nullptr;
}

// pax ctid mapping functions

void cbdb::InitCommandResource() {
  CBDB_WRAP_START;
  { paxc::init_command_resource(); }
  CBDB_WRAP_END;
}
void cbdb::ReleaseCommandResource() {
  CBDB_WRAP_START;
  { paxc::release_command_resource(); }
  CBDB_WRAP_END;
}

void cbdb::GetTableIndexAndTableNumber(Oid table_rel_oid, uint8 *table_no,
                                       uint32 *table_index) {
  CBDB_WRAP_START;
  {
    paxc::get_table_index_and_table_number(table_rel_oid, table_no,
                                           table_index);
  }
  CBDB_WRAP_END;
}

uint32 cbdb::GetBlockNumber(Oid table_rel_oid, uint32 table_index,
                            paxc::PaxBlockId block_id) {
  CBDB_WRAP_START;
  { return paxc::get_block_number(table_rel_oid, table_index, block_id); }
  CBDB_WRAP_END;
}
paxc::PaxBlockId cbdb::GetBlockId(Oid table_rel_oid, uint8 table_no,
                                  uint32 block_number) {
  CBDB_WRAP_START;
  { return paxc::get_block_id(table_rel_oid, table_no, block_number); }
  CBDB_WRAP_END;
}
