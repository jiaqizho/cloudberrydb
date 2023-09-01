#include <gtest/gtest.h>

#include "storage/pax.h"

#include <string>
#include <utility>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"

namespace pax::tests {
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

const char *pax_file_name = "./test.pax";
#define COLUMN_NUMS 2

CTupleSlot *CreateFakeCTupleSlot(bool with_value) {
  TupleTableSlot *tuple_slot;

  auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
      sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));

  tuple_desc->natts = COLUMN_NUMS;
  tuple_desc->attrs[0] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_desc->attrs[1] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  if (with_value) {
    bool *fake_is_null = new bool[COLUMN_NUMS];

    fake_is_null[0] = false;
    fake_is_null[1] = false;

    tuple_slot->tts_values[0] = Int32GetDatum(1);
    tuple_slot->tts_values[1] = Int32GetDatum(2);
    tuple_slot->tts_isnull = fake_is_null;
  }

  auto ctuple_slot = new CTupleSlot(tuple_slot);

  return ctuple_slot;
}

class MockReaderInterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  explicit MockReaderInterator(
      const std::vector<MicroPartitionMetadata> &meta_info_list)
      : index_(0) {
    micro_partitions_.insert(micro_partitions_.end(), meta_info_list.begin(),
                             meta_info_list.end());
  }

  bool HasNext() override { return index_ < micro_partitions_.size(); }

  void Rewind() override { index_ = 0; }

  MicroPartitionMetadata Next() override { return micro_partitions_[index_++]; }

 private:
  uint32 index_;
  std::vector<MicroPartitionMetadata> micro_partitions_;
};

class MockWriter : public TableWriter {
 public:
  MockWriter(const Relation relation, WriteSummaryCallback callback)
      : TableWriter(relation) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
};

class PaxWriterTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance();
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "OrcTestResourceOwner");
  }

  void TearDown() override {
    std::remove(pax_file_name);
    ResourceOwner tmp_resource_owner = CurrentResourceOwner;
    CurrentResourceOwner = NULL;
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_BEFORE_LOCKS,
                         false, true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_LOCKS, false,
                         true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_AFTER_LOCKS,
                         false, true);
    ResourceOwnerDelete(tmp_resource_owner);
  }
};

TEST_F(PaxWriterTest, WriteReadTuple) {
  CTupleSlot *slot = CreateFakeCTupleSlot(true);

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(pax_file_name));

  writer->Open();

  writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  cbdb::Pfree(slot->GetTupleTableSlot());
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(pax_file_name);
  meta_info.SetMicroPartitionId(pax_file_name);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = 0;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateFakeCTupleSlot(true);

  reader->ReadTuple(rslot);

  ASSERT_EQ(1, cbdb::DatumToInt32(rslot->GetTupleTableSlot()->tts_values[0]));
  ASSERT_EQ(2, cbdb::DatumToInt32(rslot->GetTupleTableSlot()->tts_values[1]));
  delete relation;
  delete reader;
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile) {
  CTupleSlot *slot = CreateFakeCTupleSlot(true);
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));

  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  uint32 call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return pax_file_name + std::to_string(call_times++);
      }));

  writer->Open();

  ASSERT_TRUE(writer->GetFileSplitStrategy()->SplitTupleNumbers());
  auto split_size = writer->GetFileSplitStrategy()->SplitTupleNumbers();

  for (size_t i = 0; i < split_size + 1; i++) writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  cbdb::Pfree(slot->GetTupleTableSlot());
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info1;
  meta_info1.SetMicroPartitionId(std::string(pax_file_name));
  meta_info1.SetFileName(pax_file_name + std::to_string(0));

  MicroPartitionMetadata meta_info2;
  meta_info2.SetMicroPartitionId(std::string(pax_file_name));
  meta_info2.SetFileName(pax_file_name + std::to_string(1));

  meta_info_list.push_back(std::move(meta_info1));
  meta_info_list.push_back(std::move(meta_info2));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{.build_bitmap = false,
                                            .rel_oid = 0};
  reader_options.build_bitmap = false;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateFakeCTupleSlot(true);

  for (size_t i = 0; i < split_size + 1; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    ASSERT_EQ(1, cbdb::DatumToInt32(rslot->GetTupleTableSlot()->tts_values[0]));
    ASSERT_EQ(2, cbdb::DatumToInt32(rslot->GetTupleTableSlot()->tts_values[1]));
  }
  ASSERT_FALSE(reader->ReadTuple(rslot));
  reader->Close();

  delete reader;
  delete relation;

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
}

}  // namespace pax::tests
