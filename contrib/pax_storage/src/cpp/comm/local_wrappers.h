#pragma once
extern "C" {
#include "postgres.h"  // NOLINT
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
}

namespace cbdb {
HTAB *HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                 int flags);
void *HashSearch(HTAB *hashp, const void *keyPtr, HASHACTION action,
                 bool *foundPtr);
MemoryContext AllocSetCtxCreate(MemoryContext parent, const char *name,
                                Size minContextSize, Size initBlockSize,
                                Size maxBlockSize);
void MemoryCtxRegisterResetCallback(MemoryContext context,
                                    MemoryContextCallback *cb);

Oid RelationGetRelationId(Relation rel);
}  // namespace cbdb
