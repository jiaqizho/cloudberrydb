#pragma once
#include <cstddef>


#ifndef PAX_INDEPENDENT_MODE

extern "C" {
#include "postgres.h" // NOLINT
#include "utils/palloc.h"
}
#else

struct MemoryContext {};

#endif

namespace cbdb {
    void *MemCtxAlloc(MemoryContext ctx, size_t size);
    void *Palloc(size_t size);
    void Pfree(void *ptr);
}  // namespace cbdb

// override the default new/delete to use current memory context
extern void *operator new(std::size_t size);
extern void *operator new[](std::size_t size);
extern void operator delete(void* ptr);
extern void operator delete[](void* ptr);

// specify memory context for this allocation without switching memory context
extern void *operator new(std::size_t size, MemoryContext ctx);
extern void *operator new[](std::size_t size, MemoryContext ctx);
