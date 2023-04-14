#include "comm/local_wrappers.h"

#include <cstdlib>

#ifdef PAX_INDEPENDENT_MODE

namespace cbdb {

void ResourceOwnerRememberFile(ResourceOwner owner, int file) {}
void ResourceOwnerForgetFile(ResourceOwner owner, int file) {}

void *MemCtxAlloc([[maybe_unused]] MemoryContext ctx, size_t size) {
  return malloc(size);
}

void *Palloc(size_t size) { return malloc(size); }

void Pfree(void *ptr) { return free(ptr); }
}  // namespace cbdb

void *operator new(std::size_t size) { return malloc(size); }

void *operator new[](std::size_t size) { return malloc(size); }

void *operator new(std::size_t size, [[maybe_unused]] MemoryContext ctx) {
  return malloc(size);
}

void *operator new[](std::size_t size, [[maybe_unused]] MemoryContext ctx) {
  return malloc(size);
}

void operator delete(void *ptr) { free(ptr); }

void operator delete[](void *ptr) { free(ptr); }

HTAB *cbdb::HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                       int flags) {
  return hash_create(tabname, nelem, info, flags);
}

void *cbdb::HashSearch(HTAB *hashp, const void *keyPtr, HASHACTION action,
                       bool *foundPtr) {
  return hash_search(hashp, keyPtr, action, foundPtr);
}

MemoryContext cbdb::AllocSetCtxCreate(MemoryContext parent, const char *name,
                                      Size minContextSize, Size initBlockSize,
                                      Size maxBlockSize) {
  return AllocSetContextCreateInternal(parent, name, minContextSize,
                                       initBlockSize, maxBlockSize);
}

void cbdb::MemoryCtxRegisterResetCallback(MemoryContext context,
                                          MemoryContextCallback *cb) {
  MemoryContextRegisterResetCallback(context, cb);
}

Oid cbdb::RelationGetRelationId(Relation rel) { return RelationGetRelid(rel) }
#endif  // #ifdef PAX_INDEPENDENT_MODE
