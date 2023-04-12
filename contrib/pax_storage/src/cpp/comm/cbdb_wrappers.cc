#include "comm/cbdb_wrappers.h"

#ifndef PAX_INDEPENDENT_MODE

extern "C" {
#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "cdb/cdbcustomam.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h"
#include "postgres.h"  // NOLINT
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "uuid/uuid.h"
}

namespace cbdb {
void *Palloc(size_t size);
void *Palloc0(size_t size);
void Pfree(void *ptr);
void *MemCtxAlloc(MemoryContext ctx, size_t size);
}  // namespace cbdb

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
    { return palloc(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::Palloc0(size_t size) {
  CBDB_WRAP_START;
  {
    { return palloc0(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void cbdb::Pfree(void *ptr) {
  CBDB_WRAP_START;
  {
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

#endif  // #ifndef PAX_INDEPENDENT_MODE
HTAB *cbdb::HashCreate(const char *tabname, long nelem, const HASHCTL *info,
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

struct varlena *cbdb::PgDeToastDatumPacked(struct varlena *datum) {
  CBDB_WRAP_START;
  { return pg_detoast_datum_packed(datum); }
  CBDB_WRAP_END;
  return nullptr;
}
