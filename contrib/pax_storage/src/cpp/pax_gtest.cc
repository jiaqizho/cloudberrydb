#include <cstdio>
#include "comm/gtest_wrappers.h"

#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pax_fastsequence.h"
#include "comm/cbdb_wrappers.h"
#include "storage/oper/pax_stats.h"
#include "stub.h"

bool MockMinMaxGetStrategyProcinfo(Oid, Oid, Oid *, FmgrInfo *,
                                   StrategyNumber) {
  return false;
}

int32 MockGetFastSequences(Oid) {
  static int32 mock_id = 0;
  return mock_id++;
}

void MockInsertMicroPartitionPlaceHolder(Oid, int) {
  // do nothing
}

void MockDeleteMicroPartitionEntry(Oid, Snapshot, int) {
  // do nothing
}

void MockExecStoreVirtualTuple(TupleTableSlot *) {
  // do nothing
}

std::string MockBuildPaxDirectoryPath(RelFileNode, BackendId) {
  return std::string(".");
}

std::vector<int> MockGetMinMaxColumnIndexes(Relation rel) {
  return std::vector<int>();
}

std::vector<int> MockBloomFilterColumnIndexes(Relation rel) {
  return std::vector<int>();
}

std::vector<std::tuple<pax::ColumnEncoding_Kind, int>> MockGetRelEncodingOptions(Relation rel) {
  return std::vector<std::tuple<pax::ColumnEncoding_Kind, int>>();
}

char MockGetTyptype(Oid typid) {
  return TYPTYPE_BASE;
}

// Mock global method which is not link from another libarays
void GlobalMock(Stub *stub) {
  stub->set(pax::MinMaxGetPgStrategyProcinfo, MockMinMaxGetStrategyProcinfo);
  stub->set(paxc::CPaxGetFastSequences, MockGetFastSequences);
  stub->set(cbdb::BuildPaxDirectoryPath, MockBuildPaxDirectoryPath);
  stub->set(cbdb::InsertMicroPartitionPlaceHolder,
            MockInsertMicroPartitionPlaceHolder);
  stub->set(cbdb::DeleteMicroPartitionEntry, MockDeleteMicroPartitionEntry);
  stub->set(cbdb::GetMinMaxColumnIndexes, MockGetMinMaxColumnIndexes);
  stub->set(cbdb::GetBloomFilterColumnIndexes, MockBloomFilterColumnIndexes);
  stub->set(cbdb::GetRelEncodingOptions, MockGetRelEncodingOptions);
  stub->set(cbdb::GetTyptype, MockGetTyptype);
  stub->set(ExecStoreVirtualTuple, MockExecStoreVirtualTuple);
}

int main(int argc, char **argv) {
  Stub *stub_global;
  int rc;
  MemoryContextInit();
  stub_global = new Stub();

  testing::InitGoogleTest(&argc, argv);
  GlobalMock(stub_global);

  rc = RUN_ALL_TESTS();
  MemoryContextReset(TopMemoryContext);
  return rc;
}
