#pragma once
#include "comm/cbdb_api.h"
#include <memory>
#include <type_traits>

namespace pax {

// class tag for object that needs to release its memory resource
// in the destructor function.
class MemoryObject {
public:
 virtual ~MemoryObject() = default;
};

template <typename T=void>
static inline T* PAX_ALLOC(size_t size) {
  auto p = malloc(size);
  if (!p) throw std::bad_alloc{};

  return reinterpret_cast<T *>(p);
}

template <typename T=void>
static inline T* PAX_ALLOC0(size_t size) {
  auto p = PAX_ALLOC<T>(size);
  memset(reinterpret_cast<void *>(p), 0, size);
  return p;
}

template <typename T=void>
static inline T* PAX_REALLOC(void *ptr, size_t new_size) {
  auto p = realloc(reinterpret_cast<void *>(ptr), new_size);
  if (!p) throw std::bad_alloc{};

  return reinterpret_cast<T *>(p);
}

template <typename T=void>
static inline void PAX_FREE(const T *ptr) {
  T *p = const_cast<T *>(ptr);
  if (p) free(reinterpret_cast<void *>(p));
}

template <typename T, typename... Args>
static inline T* PAX_NEW(Args&&... args) {
  return new T(std::forward<Args>(args)...);
}

template <typename T>
static inline T* PAX_NEW_ARRAY(size_t N) {
  return new T[N];
}

template <typename T>
static inline void PAX_DELETE(T *obj) {
  delete obj;
}

template <typename T>
static inline void PAX_DELETE(const T *obj) {
  delete obj;
}

template <typename T>
static inline void PAX_DELETE_ARRAY(const T *obj) {
  delete[] obj;
}

template <typename T>
static inline void PAX_DELETE_ARRAY(T * const &obj) {
  delete []obj;
}

template <typename T>
static inline void ReleaseTopObject(Datum arg) {
  auto obj = reinterpret_cast<T *>(DatumGetPointer(arg));
  Assert(obj);

  PAX_DELETE(obj);
}

}

