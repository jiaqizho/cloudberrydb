#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"

#ifndef PAX_INDEPENDENT_MODE

extern "C" {
#include "utils/elog.h"
}

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
        : m_global_exception_stack(global_exception_stack)
        , m_global_error_context_stack(global_error_context_stack)
        , m_exception_stack(*global_exception_stack)
        , m_error_context_stack(*global_error_context_stack)
    {}

    // dtor
    ~CAutoExceptionStack() {
        *m_global_exception_stack = m_exception_stack;
        *m_global_error_context_stack = m_error_context_stack;
    }

    // set the exception stack to the given address
    void SetLocalJmp(void *local_jump) {
        *m_global_exception_stack = local_jump;
    }
};  // class CAutoExceptionStack

}  // namespace cbdb

#define CBDB_WRAP_START                                             \
    sigjmp_buf local_sigjmp_buf;                                    \
    {                                                               \
        CAutoExceptionStack aes(                                    \
            reinterpret_cast<void **>(&PG_exception_stack),         \
            reinterpret_cast<void **>(&error_context_stack));       \
        if (0 == sigsetjmp(local_sigjmp_buf, 0))                    \
        {                                                           \
            aes.SetLocalJmp(&local_sigjmp_buf)

#define CBDB_WRAP_END                                               \
    }                                                               \
    else                                                            \
    {                                                               \
        CBDB_RAISE(cbdb::CException::ExType::ExTypeCError);         \
    }                                                               \
    }

void *cbdb::MemCtxAlloc(MemoryContext ctx, size_t size) {
    CBDB_WRAP_START;
    {
        return MemoryContextAlloc(ctx, (Size)size);
    }
    CBDB_WRAP_END;
    return nullptr;
}

void *cbdb::Palloc(size_t size) {
    CBDB_WRAP_START;
    {
        return palloc(size);
    }
    CBDB_WRAP_END;
    return nullptr;
}

void cbdb::Pfree(void *ptr) {
    CBDB_WRAP_START;
    {
        pfree(ptr);
    }
    CBDB_WRAP_END;
}

void *operator new(std::size_t size) {
    return cbdb::Palloc(size);
}

void *operator new[](std::size_t size) {
    return cbdb::Palloc(size);
}

void *operator new(std::size_t size, MemoryContext ctx) {
    return cbdb::MemCtxAlloc(ctx, size);
}

void *operator new[](std::size_t size, MemoryContext ctx) {
    return cbdb::MemCtxAlloc(ctx, size);
}

void operator delete(void* ptr) {
    cbdb::Pfree(ptr);
}

void operator delete[](void* ptr) {
    cbdb::Pfree(ptr);
}
