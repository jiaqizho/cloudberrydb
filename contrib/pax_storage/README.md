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
