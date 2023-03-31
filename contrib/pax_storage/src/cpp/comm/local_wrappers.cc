#include <cstdlib>
#include "comm/cbdb_wrappers.h"

namespace cbdb {
    void *MemCtxAlloc([[maybe_unused]] MemoryContext ctx, size_t size) {
        return malloc(size);
    }

    void *Palloc(size_t size) {
        return malloc(size);
    }

    void Pfree(void *ptr) {
        return free(ptr);
    }
}  // namespace cbdb

void *operator new(std::size_t size) {
    return malloc(size);
}

void *operator new[](std::size_t size) {
    return malloc(size);
}

void *operator new(std::size_t size, [[maybe_unused]] MemoryContext ctx) {
    return malloc(size);
}

void *operator new[](std::size_t size, [[maybe_unused]] MemoryContext ctx) {
    return malloc(size);
}

void operator delete(void* ptr) {
    free(ptr);
}

void operator delete[](void* ptr) {
    free(ptr);
}
