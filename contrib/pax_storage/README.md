# Micro Partition

## Overview

- Storage
  - Provides efficient data access services for the computing layer, which is more conducive to the data format processed by the computing layer
  - Multiple data format support, data openness (third-party tools can be directly read through files)
- Cache
  - Provides consistent caching for external table 
  - Cache consistency
- Dynamic expansion/contraction (second level)
- Compatible with CBDB non-cloud version code
 

## Configuration and build

You **must** enable the pre-push hooks to automatically check format:

```
cp .githooks/* .git/hooks/`
```

### Build PAX

1. make sure you have already build and install `cbdb` in your env
2. already `source greenplum_path.sh`
3. follow below steps

```
mkdir build
cd build
cmake .. 
make -j 
```

### Build GTEST

1. make sure already build pax with cmake option `-DBUILD_GTEST=on`, default value is on
2. better with debug cmake option `-DENBALE_DEBUG=on`, default value is on
3. run tests

```
cd build
./src/cpp/test_main
```

### Build extension

1. After build PAX, `pax.so` will be generated in `src/data`
2. follow below steps

```
cd src/data
make install -j
```

## GTEST accesses internal functions/variables

Using marco `RUN_GTEST` to make protected/private functions/variables public.
ex. 

**obj.h**:

```
class A {
  public:
    function a();

#ifndef RUN_GTEST
  protected:
#endif 
    function b();

#ifndef RUN_GTEST
  private:
#endif
    int c;
}
```

**obj_test.cc**:

```
#include "obj.h"

TEST_F(Example, test) {
  A a;
  a.a(); // access public function
  a.b(); // access protected function
  a.c; // access private variables
}
```

### exception && try catch

There are two way to throw a exception
1. `CBDB_RAISE(ExType)`: direct throw
2. `CBDB_CHECK(check_exp, ExType)`: check failed, then throw

About try catch, you need to know 
  1. Expected exceptions, catcher can handle it.
    - Do not `rethrow` it
    - Should do `try...catch` to handle it
    - Better not to write logic in `try...catch`, but use the return value to cover the logic
    - like: network problem...
  2. Unexpected exceptions
    - Thinking should we add `try...catch` to handle it?
      - have some global resources must do `try...catch` to release it
        - memory in top memory context or session memory context
        - opened fd
        - static resources
      - do not have any global resources
        - just throw it without `try...catch`
    - like: logic error, out of range error...

example here:
1. Expected exceptions

```
void RequestResources(bool should_retry) {
  RequestTempArgs args;

  // have some alloc in it
  InitRequestTempArgs(&args); 

  try {
    DoHttpRequest();
  } catch {
    // free the resource and retry 
    DestroyRequestTempArgs(&args);
    if (should_retry) {
      RequestResources(false);
    }
  }
}

```

2. Unexpected exceptions with global resources

```
static ReadContext *context;

void InitReadContext() {
  context = palloc(TopMemoryContext, sizeof(ReadContext));
}

void ReadResources() {
  Assert(context);
  try {
    ReadResource(context);
  } catch {
    // should destroy global resource
    // otherwise, will got resource leak in InitReadContext()
    DestroyReadContext(context);
    throw CurrentException();
  }
}
```

3. Unexpected exceptions without global resources

```
void ParseResource(Resource * res, size_t offset) {
  // direct throw without any try...catch
  CBDB_CHECK(offset > res->size(), ExTypeOutOfRange);
  ... // normal logic
}
```
