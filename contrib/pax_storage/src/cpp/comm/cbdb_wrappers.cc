#include "comm/cbdb_wrappers.h"

extern "C" {
const char *progname;
};

void *cbdb::MemCtxAlloc(MemoryContext ctx, size_t size) {
  CBDB_WRAP_START;
  {
    { return MemoryContextAlloc(ctx, (Size)size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::Palloc(size_t size) {
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

void *cbdb::Palloc0(size_t size) {
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

void cbdb::Pfree(void *ptr) {
  CBDB_WRAP_START;
  {
#ifdef RUN_GTEST
    if (ptr == nullptr) return;
#endif
    { pfree(ptr); }
  }
  CBDB_WRAP_END;
}

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

void *cbdb::HashSearch(HTAB *hashp, const void *keyPtr, HASHACTION action,
                       bool *foundPtr) {
  CBDB_WRAP_START;
  { return hash_search(hashp, keyPtr, action, foundPtr); }
  CBDB_WRAP_END;
  return nullptr;
}

MemoryContext cbdb::AllocSetCtxCreate(MemoryContext parent, const char *name,
                                      Size minContextSize, Size initBlockSize,
                                      Size maxBlockSize) {
  CBDB_WRAP_START;
  {
    return AllocSetContextCreateInternal(parent, name, minContextSize,
                                         initBlockSize, maxBlockSize);
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

void *cbdb::PointerFromDatum(Datum d) {
  CBDB_WRAP_START;
  { return DatumGetPointer(d); }
  CBDB_WRAP_END;
  return nullptr;
}

int64 cbdb::Int64FromDatum(Datum d) {
  Datum d2 = d;
  CBDB_WRAP_START;
  { return DatumGetInt64(d2); }
  CBDB_WRAP_END;
  return 0;
}

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

struct varlena *cbdb::PgDeToastDatumPacked(struct varlena *datum) {
  CBDB_WRAP_START;
  { return pg_detoast_datum_packed(datum); }
  CBDB_WRAP_END;
  return nullptr;
}
