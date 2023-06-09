#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/pax.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"

namespace pax::tests {
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

const char *pax_file_name = "./test.pax";
#define COLUMN_NUMS 2

TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc,
                                   const TupleTableSlotOps *tts_ops) {
  Size basesz, allocsz;
  TupleTableSlot *slot;

  basesz = tts_ops->base_slot_size;

  if (tupleDesc)
    allocsz = MAXALIGN(basesz) + MAXALIGN(tupleDesc->natts * sizeof(Datum)) +
              MAXALIGN(tupleDesc->natts * sizeof(bool));
  else
    allocsz = basesz;

  slot = reinterpret_cast<TupleTableSlot *>(cbdb::Palloc0(allocsz));
  *((const TupleTableSlotOps **)&slot->tts_ops) = tts_ops;
  slot->type = T_TupleTableSlot;
  slot->tts_flags |= TTS_FLAG_EMPTY;
  if (tupleDesc != NULL) slot->tts_flags |= TTS_FLAG_FIXED;
  slot->tts_tupleDescriptor = tupleDesc;
  slot->tts_mcxt = CurrentMemoryContext;
  slot->tts_nvalid = 0;

  if (tupleDesc != NULL) {
    slot->tts_values = reinterpret_cast<Datum *>(
        (reinterpret_cast<char *>(slot)) + MAXALIGN(basesz));
    slot->tts_isnull = reinterpret_cast<bool *>(
        (reinterpret_cast<char *>(slot)) + MAXALIGN(basesz) +
        MAXALIGN(tupleDesc->natts * sizeof(Datum)));
  }
  slot->tts_ops->init(slot);

  return slot;
}

CTupleSlot *CreateFakeCTupleSlot(bool with_value) {
  TupleTableSlot *tuple_slot;

  TupleDescData *tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
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

  tuple_slot = pax::tests::MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  if (with_value) {
    bool *fake_is_null = new bool[COLUMN_NUMS];

    fake_is_null[0] = false;
    fake_is_null[1] = false;

    tuple_slot->tts_values[0] = Int32GetDatum(1);
    tuple_slot->tts_values[1] = Int32GetDatum(2);
    tuple_slot->tts_isnull = fake_is_null;
  }

  CTupleSlot *ctuple_slot = new CTupleSlot(tuple_slot);

  return ctuple_slot;
}

class MockReaderInterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  explicit MockReaderInterator(const std::vector<MicroPartitionMetadata> &meta_info_list)
      : index(0) {
    micro_partitions_.insert(micro_partitions_.end(), meta_info_list.begin(),
                              meta_info_list.end());
  }

  void Init() override {}

  bool HasNext() const override {
    if (micro_partitions_.size() == 0) {
      return false;
    }
    return index < micro_partitions_.size() - 1;
  }

  MicroPartitionMetadata Current() const override {
    return micro_partitions_[index];
  }
  virtual bool Empty() const { return micro_partitions_.empty(); }
  virtual uint32_t Size() const { return micro_partitions_.size(); }
  virtual size_t Seek(int offset, IteratorSeekPosType whence) {
    switch (whence) {
      case BEGIN:
        index = offset;
        break;
      case CURRENT:
        index += offset;
        break;
      case END:
        index = micro_partitions_.size() - offset;
        break;
    }
    return index;
  }

  MicroPartitionMetadata Next() override {
    return micro_partitions_[++index];
  }

 private:
  uint32_t index;
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
  void SetUp() override { Singleton<LocalFileSystem>::GetInstance(); }

  void TearDown() override { std::remove(pax_file_name); }
};

TEST_F(PaxWriterTest, WriteReadTuple) {
  CTupleSlot *slot = CreateFakeCTupleSlot(true);

  Relation relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary &summary) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(pax_file_name));

  writer->Open();

  writer->WriteTuple(slot);
  ASSERT_EQ(1, writer->GetTotalTupleNumbers());
  writer->Close();
  ASSERT_TRUE(callback_called);

  cbdb::Pfree(slot->GetTupleTableSlot());
  delete writer;

  FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReaderPtr micro_partition_reader =
      new OrcIteratorReader(file_system);

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info(pax_file_name, pax_file_name);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options;
  reader_options.build_bitmap_ = false;
  reader_options.rel_oid_ = 0;
  reader = new TableReader(micro_partition_reader,
                           std::move(meta_info_iterator), reader_options);
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
  Relation relation = (Relation)cbdb::Palloc0(sizeof(RelationData));

  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary &summary) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  uint32_t call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return pax_file_name + std::to_string(call_times++);
      }));

  writer->Open();

  ASSERT_NE(-1, writer->GetFileSplitStrategy()->SplitTupleNumbers());
  auto split_size = writer->GetFileSplitStrategy()->SplitTupleNumbers();

  for (size_t i = 0; i < split_size + 1; i++) writer->WriteTuple(slot);
  ASSERT_EQ(split_size + 1, writer->GetTotalTupleNumbers());
  writer->Close();
  ASSERT_TRUE(callback_called);

  cbdb::Pfree(slot->GetTupleTableSlot());
  delete writer;

  FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReaderPtr micro_partition_reader =
      new OrcIteratorReader(file_system);

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info1(pax_file_name,
                                    pax_file_name + std::to_string(0));

  MicroPartitionMetadata meta_info2(pax_file_name,
                                    pax_file_name + std::to_string(1));

  meta_info_list.push_back(std::move(meta_info1));
  meta_info_list.push_back(std::move(meta_info2));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options;
  reader_options.build_bitmap_ = false;
  reader = new TableReader(micro_partition_reader,
                           std::move(meta_info_iterator), reader_options);
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
