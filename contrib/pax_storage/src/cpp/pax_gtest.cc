#include <cstdio>

#include "stub.h"
#include "comm/gtest_wrappers.h"
#include "comm/cbdb_wrappers.h"

bool MockMinMaxGetStrategyProcinfo(Oid, Oid, Oid *, FmgrInfo *, StrategyNumber) {
  return false;
}

// Mock global method which is not link from another libarays
void GlobalMock(Stub * stub) {
  stub->set(cbdb::MinMaxGetStrategyProcinfo, MockMinMaxGetStrategyProcinfo);
}

int main() {
  Stub *stub_global;
  stub_global = new Stub();
  testing::InitGoogleTest();
  GlobalMock(stub_global);

  return RUN_ALL_TESTS();
}
