#include "comm/cbdb_wrappers.h"

#include "storage/paxc_block_map_manager.h"
extern "C" {
const char *progname;
}

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

void *cbdb::RePalloc(void *ptr, size_t size) {
  CBDB_WRAP_START;
  { return repalloc(ptr, size); }
  CBDB_WRAP_END;
  return nullptr;
}

void cbdb::Pfree(void *ptr) {
#ifdef RUN_GTEST
  if (ptr == nullptr) {
    return;
  }
#endif
  CBDB_WRAP_START;
  { pfree(ptr); }
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
    // FIXME(gongxun): is VARSIZE_ANY(d) better?
    vl = (struct varlena *)DatumGetPointer(d);
    *len = VARSIZE_ANY_EXHDR(vl) + VARHDRSZ;
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

void cbdb::GetTableIndexAndTableNumber(Oid table_rel_oid, uint8_t *table_no,
                                       uint32_t *table_index) {
  CBDB_WRAP_START;
  {
    paxc::get_table_index_and_table_number(table_rel_oid, table_no,
                                           table_index);
  }
  CBDB_WRAP_END;
}

uint32_t cbdb::GetBlockNumber(Oid table_rel_oid, uint32_t table_index,
                              paxc::PaxBlockId block_id) {
  CBDB_WRAP_START;
  { return paxc::get_block_number(table_rel_oid, table_index, block_id); }
  CBDB_WRAP_END;
}
paxc::PaxBlockId cbdb::GetBlockId(Oid table_rel_oid, uint8_t table_no,
                                  uint32_t block_number) {
  CBDB_WRAP_START;
  { return paxc::get_block_id(table_rel_oid, table_no, block_number); }
  CBDB_WRAP_END;
}
