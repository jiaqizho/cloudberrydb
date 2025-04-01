#include <gtest/gtest.h>

#include "storage/pax.h"

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "access/pax_partition.h"
#include "access/paxc_rel_options.h"
#include "comm/gtest_wrappers.h"
#include "comm/guc.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/porc.h"
#include "storage/pax_table_partition_writer.h"
#include "stub.h"

namespace pax::tests {
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;
using ::testing::ReturnRoundRobin;

const char *pax_file_name = "12";

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
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

class MockSplitStrategy final : public FileSplitStrategy {
 public:
  size_t SplitTupleNumbers() const override {
    // 1600 tuple
    return 1600;
  }

  size_t SplitFileSize() const override { return 0; }

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override {
    return num_tuples >= SplitTupleNumbers();
  }
};

class MockSplitStrategy2 final : public FileSplitStrategy {
 public:
  size_t SplitTupleNumbers() const override {
    // 10000 tuple
    return 10000;
  }

  size_t SplitFileSize() const override {
    // 32kb
    return 32 * 1024;
  }

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override {
    return num_tuples >= SplitTupleNumbers() || phy_size > SplitFileSize();
  }
};

class PaxWriterTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(pax_file_name);
    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(pax_file_name);
    ReleaseTestResourceOwner();
  }
};

TEST_F(PaxWriterTest, WriteReadTuple) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->tts_tupleDescriptor;
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(pax_file_name));

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(pax_file_name);
  meta_info.SetMicroPartitionId(pax_file_name);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);
  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  reader->ReadTuple(rslot);
  EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));

  DeleteTestTupleTableSlot(rslot);
  delete relation;
  delete reader;
}

std::vector<int> MockGetALLMinMaxColumnsIndex(Relation rel) {
  std::vector<int> minmax_columns;
  for (int i = 0; i < rel->rd_att->natts; i++) {
    minmax_columns.push_back(i);
  }
  return minmax_columns;
}

bool MockSumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                           FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                           bool *final_func_exist, FmgrInfo *add_finfo) {
  return false;
}

TEST_F(PaxWriterTest, TestOper) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  Relation relation;
  std::vector<size_t> mins;
  std::vector<size_t> maxs;
  int origin_pax_max_tuples_per_group = pax_max_tuples_per_group;
  Stub *stub;
  stub = new Stub();

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
  std::remove((pax_file_name + std::to_string(2)).c_str());

  relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = slot->tts_tupleDescriptor;

  TableWriter::WriteSummaryCallback callback =
      [&mins, &maxs](const WriteSummary &summary) {
        ASSERT_NE(summary.mp_stats, nullptr);
        ASSERT_EQ(summary.mp_stats->columnstats_size(), COLUMN_NUMS);
        auto min_ptr = reinterpret_cast<const int32 *>(
            summary.mp_stats->columnstats(2).datastats().minimal().data());
        auto max_ptr = reinterpret_cast<const int32 *>(
            summary.mp_stats->columnstats(2).datastats().maximum().data());
        ASSERT_NE(min_ptr, nullptr);
        ASSERT_NE(max_ptr, nullptr);
        mins.emplace_back(*min_ptr);
        maxs.emplace_back(*max_ptr);
      };

  auto strategy = new MockSplitStrategy();
  auto split_size = strategy->SplitTupleNumbers();

  // 8 groups in a file
  pax_max_tuples_per_group = split_size / 8;

  stub->set(cbdb::GetMinMaxColumnsIndex, MockGetALLMinMaxColumnsIndex);
  stub->set(cbdb::SumAGGGetProcinfo, MockSumAGGGetProcinfo);

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(strategy);

  uint32 call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return std::string(pax_file_name) + std::to_string(call_times++);
      }));
  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  // 3 files
  for (size_t i = 0; i < split_size * 3; i++) {
    slot->tts_values[2] = i;
    writer->WriteTuple(slot);
  }
  writer->Close();

  // verify file min/max
  ASSERT_EQ(mins.size(), 3UL);
  ASSERT_EQ(maxs.size(), 3UL);

  for (size_t i = 0; i < 3; i++) {
    ASSERT_EQ(mins[i], split_size * i);
    ASSERT_EQ(maxs[i], split_size * (i + 1) - 1);
  }

  DeleteTestTupleTableSlot(slot);
  delete writer;

  // verify stripe min/max
  auto verify_single_file = [](size_t file_index, size_t file_tuples) {
    LocalFileSystem *local_fs;
    MicroPartitionReader::ReaderOptions reader_options;
    TupleTableSlot *rslot = CreateTestTupleTableSlot(false);
    size_t file_min_max_offset = file_index * file_tuples;

    local_fs = Singleton<LocalFileSystem>::GetInstance();
    auto reader = new OrcReader(local_fs->Open(
        pax_file_name + std::to_string(file_index), fs::kReadMode));
    reader->Open(reader_options);

    ASSERT_EQ(reader->GetGroupNums(), 8UL);
    for (size_t i = 0; i < 8; i++) {
      auto stats = reader->GetGroupStatsInfo(i);
      auto min = *reinterpret_cast<const int32 *>(
          stats->DataStats(2).minimal().data());
      auto max = *reinterpret_cast<const int32 *>(
          stats->DataStats(2).maximum().data());

      EXPECT_EQ(pax_max_tuples_per_group * i + file_min_max_offset,
                static_cast<size_t>(min));
      EXPECT_EQ(pax_max_tuples_per_group * (i + 1) + file_min_max_offset - 1,
                static_cast<size_t>(max));
    }

    delete reader;
    DeleteTestTupleTableSlot(rslot);
  };

  verify_single_file(0, split_size);
  verify_single_file(1, split_size);
  verify_single_file(2, split_size);

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
  std::remove((pax_file_name + std::to_string(2)).c_str());

  delete stub;

  pax_max_tuples_per_group = origin_pax_max_tuples_per_group;
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));

  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new MockSplitStrategy());
  uint32 call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return std::string(pax_file_name) + std::to_string(call_times++);
      }));
  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  ASSERT_TRUE(writer->GetFileSplitStrategy()->SplitTupleNumbers());
  auto split_size = writer->GetFileSplitStrategy()->SplitTupleNumbers();

  for (size_t i = 0; i < split_size + 1; i++) {
    writer->WriteTuple(slot);
  }
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
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
  TableReader::ReaderOptions reader_options;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  for (size_t i = 0; i < split_size + 1; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));
  }
  ASSERT_FALSE(reader->ReadTuple(rslot));
  reader->Close();

  DeleteTestTupleTableSlot(rslot);
  delete reader;
  delete relation;

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile2) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  int origin_pax_max_tuples_per_group = pax_max_tuples_per_group;
  pax_max_tuples_per_group = 100;

  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);

  writer->SetFileSplitStrategy(new MockSplitStrategy2());
  uint32 call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return std::string(pax_file_name) + std::to_string(call_times++);
      }));
  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  // The length of each tuple is 212 bytes
  // The file size limit is 32kb
  // Should write 2 files here
  for (size_t i = 0; i < 200; i++) {
    writer->WriteTuple(slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
  delete writer;

  auto file_size = [](std::string file_name) {
    std::ifstream in(file_name, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
  };
  auto file1_size = file_size(std::string(pax_file_name) + std::to_string(0));
  EXPECT_GT(file1_size, 0);
  EXPECT_TRUE(file1_size < (32 * 1024 * 1.15) &&
              file1_size > (32 * 1024 * 0.85));

  std::remove((std::string(pax_file_name) + std::to_string(0)).c_str());
  std::remove((std::string(pax_file_name) + std::to_string(1)).c_str());
  // set back to pax_max_tuples_per_group
  pax_max_tuples_per_group = origin_pax_max_tuples_per_group;
}

class MockParitionWriter : public TableParitionWriter {
 public:
  MockParitionWriter(const Relation relation, PartitionObject *bucket,
                     WriteSummaryCallback callback)
      : TableParitionWriter(relation, bucket) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

namespace mock_partition_test {
int NumPartitions() { return 10; }
int FindPartition(TupleTableSlot * /*slot*/) {
  static int round = 0;
  auto index = round % 8;
  static std::vector<int> round_indexs = {0, 2, 3, 4, 5, 6, 8, 9};
  ++round;
  return round_indexs[index];
};

std::vector<std::vector<size_t>> GetPartitionMergeInfos() {
  return {{0, 2, 3}, {4, 5}, {6, 8, 9}};
}

void MicroPartitionStatsMerge(MicroPartitionStats *stats, TupleDesc desc) {}
}  // namespace mock_partition_test

TEST_F(PaxWriterTest, ParitionWriteReadTuple) {
  std::vector<const char *> file_names = {
      "80000", "80002", "80003", "80004", "80005", "80006", "80008", "80009",
  };
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;
  Stub *stub;
  stub = new Stub();

  auto clear_disk_file = [file_names] {
    for (const auto &file_name : file_names) {
      std::remove(file_name);
    }
  };

  clear_disk_file();

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }

  stub->set(ADDR(PartitionObject, NumPartitions),
            mock_partition_test::NumPartitions);
  stub->set(ADDR(PartitionObject, FindPartition),
            mock_partition_test::FindPartition);
  stub->set(ADDR(MockParitionWriter, GetPartitionMergeInfos),
            mock_partition_test::GetPartitionMergeInfos);
  stub->set(ADDR(MicroPartitionStats, MergeTo),
            mock_partition_test::MicroPartitionStatsMerge);

  auto part_obj = new PartitionObject();
  auto writer = new MockParitionWriter(relation, part_obj, callback);

  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(8)  // must be 8
      .WillRepeatedly(ReturnRoundRobin(file_names));

  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  for (size_t i = 0; i < 400; i++) {
    writer->WriteTuple(slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
  delete part_obj;
  delete writer;

  // will remain pax_parition_0.file, pax_parition_4.file,
  // pax_parition_6.file after merge
  ASSERT_EQ(0, access(file_names[0], 0));
  ASSERT_NE(0, access(file_names[1], 0));
  ASSERT_NE(0, access(file_names[2], 0));
  ASSERT_EQ(0, access(file_names[3], 0));
  ASSERT_NE(0, access(file_names[4], 0));
  ASSERT_EQ(0, access(file_names[5], 0));
  ASSERT_NE(0, access(file_names[6], 0));
  ASSERT_NE(0, access(file_names[7], 0));

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;
  meta_info.SetFileName(file_names[0]);
  meta_info.SetMicroPartitionId(file_names[0]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[3]);
  meta_info.SetMicroPartitionId(file_names[3]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[5]);
  meta_info.SetMicroPartitionId(file_names[5]);
  meta_info_list.push_back(meta_info);

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  for (int i = 0; i < 400; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));
  }

  DeleteTestTupleTableSlot(rslot);
  delete relation;
  delete reader;
  delete stub;

  clear_disk_file();
}

TEST_F(PaxWriterTest, ParitionWriteReadTupleWithToast) {
  std::vector<const char *> file_names = {
      "80000", "80002", "80003", "80004", "80005", "80006", "80008", "80009",
  };
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  bool callback_called = false;
  Stub *stub;
  stub = new Stub();

  TupleTableSlot *tuple_slot = nullptr, *tuple_slot_empty = nullptr;
  TupleDesc tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) +
                    sizeof(FormData_pg_attribute) * TOAST_COLUMN_NUMS));

  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = tuple_desc;

  char no_toast_buff[NO_TOAST_COLUMN_SIZE] = {0};
  char compress_toast_buff[COMPRESS_TOAST_COLUMN_SIZE] = {0};
  char external_toast_buff[EXTERNAL_TOAST_COLUMN_SIZE] = {0};
  char external_compress_toast_buff[EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE] = {0};

  int origin_pax_min_size_of_compress_toast = pax_min_size_of_compress_toast;
  int origin_pax_min_size_of_external_toast = pax_min_size_of_external_toast;

  pax_min_size_of_compress_toast = 512;
  pax_min_size_of_external_toast = 1024;

  tuple_desc->natts = TOAST_COLUMN_NUMS;
  tuple_desc->attrs[0] = {.atttypid = TEXTOID,
                          .attlen = -1,
                          .attbyval = false,
                          .attalign = TYPALIGN_DOUBLE,
                          .attstorage = TYPSTORAGE_EXTENDED,
                          .attisdropped = false,
                          .attcollation = DEFAULT_COLLATION_OID};
  tuple_desc->attrs[1] = tuple_desc->attrs[0];
  tuple_desc->attrs[2] = tuple_desc->attrs[0];
  tuple_desc->attrs[3] = tuple_desc->attrs[0];
  // column 4 is external but no compress
  tuple_desc->attrs[3].attstorage = TYPSTORAGE_EXTERNAL;

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  GenTextBuffer(no_toast_buff + VARHDRSZ, NO_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(compress_toast_buff + VARHDRSZ,
                COMPRESS_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(external_toast_buff + VARHDRSZ,
                EXTERNAL_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(external_compress_toast_buff + VARHDRSZ,
                EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE - VARHDRSZ);

  SET_VARSIZE(no_toast_buff, NO_TOAST_COLUMN_SIZE);
  SET_VARSIZE(compress_toast_buff, COMPRESS_TOAST_COLUMN_SIZE);
  SET_VARSIZE(external_toast_buff, EXTERNAL_TOAST_COLUMN_SIZE);
  SET_VARSIZE(external_compress_toast_buff,
              EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE);

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;
  tuple_slot->tts_isnull[2] = false;
  tuple_slot->tts_isnull[3] = false;

  tuple_slot->tts_values[0] = PointerGetDatum(no_toast_buff);
  tuple_slot->tts_values[1] = PointerGetDatum(compress_toast_buff);
  tuple_slot->tts_values[2] = PointerGetDatum(external_compress_toast_buff);
  tuple_slot->tts_values[3] = PointerGetDatum(external_toast_buff);

  auto clear_disk_file = [file_names] {
    for (auto file_name : file_names) {
      std::remove(file_name);
      auto toast_file_name = std::string(file_name) + ".toast";
      std::remove(toast_file_name.c_str());
    }
  };

  clear_disk_file();

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }

  stub->set(ADDR(PartitionObject, NumPartitions),
            mock_partition_test::NumPartitions);
  stub->set(ADDR(PartitionObject, FindPartition),
            mock_partition_test::FindPartition);
  stub->set(ADDR(MockParitionWriter, GetPartitionMergeInfos),
            mock_partition_test::GetPartitionMergeInfos);
  stub->set(ADDR(MicroPartitionStats, MergeTo),
            mock_partition_test::MicroPartitionStatsMerge);

  auto part_obj = new PartitionObject();
  auto writer = new MockParitionWriter(relation, part_obj, callback);

  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(8)  // must be 8
      .WillRepeatedly(ReturnRoundRobin(file_names));

  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  for (size_t i = 0; i < 400; i++) {
    writer->WriteTuple(tuple_slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  delete part_obj;
  delete writer;

  // will remain pax_parition_0.file, pax_parition_4.file,
  // pax_parition_6.file after merge
  ASSERT_EQ(0, access(file_names[0], 0));
  ASSERT_NE(0, access(file_names[1], 0));
  ASSERT_NE(0, access(file_names[2], 0));
  ASSERT_EQ(0, access(file_names[3], 0));
  ASSERT_NE(0, access(file_names[4], 0));
  ASSERT_EQ(0, access(file_names[5], 0));
  ASSERT_NE(0, access(file_names[6], 0));
  ASSERT_NE(0, access(file_names[7], 0));

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;
  meta_info.SetFileName(file_names[0]);
  meta_info.SetMicroPartitionId(file_names[0]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[3]);
  meta_info.SetMicroPartitionId(file_names[3]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[5]);
  meta_info.SetMicroPartitionId(file_names[5]);
  meta_info_list.push_back(meta_info);

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  tuple_slot_empty = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  for (int i = 0; i < 400; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot_empty));

    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[0]), 10UL + 4);
    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 512UL + 4);
    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 1024UL + 4);
    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[3]), 768UL + 4);

    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[0]),
                       no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                       compress_toast_buff, COMPRESS_TOAST_COLUMN_SIZE) == 0);
    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                       external_compress_toast_buff,
                       EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[3]),
                       external_toast_buff, EXTERNAL_TOAST_COLUMN_SIZE) == 0);
  }

  DeleteTestTupleTableSlot(tuple_slot);
  DeleteTestTupleTableSlot(tuple_slot_empty);
  delete relation;
  delete reader;
  delete stub;

  clear_disk_file();

  pax_min_size_of_compress_toast = origin_pax_min_size_of_compress_toast;
  pax_min_size_of_external_toast = origin_pax_min_size_of_external_toast;
}

namespace exceptions {

TEST_F(PaxWriterTest, WriteReadException) {
  bool get_exception = false;
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->tts_tupleDescriptor;
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(pax_file_name));

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(pax_file_name);
  meta_info.SetMicroPartitionId(pax_file_name);
  meta_info.SetTupleCount(1);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);
  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  try {
    reader->GetTuple(rslot, ForwardScanDirection, 100);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("No more tuples to read."), std::string::npos);
    ASSERT_NE(exception_str.find("target offsets=100"), std::string::npos);
    ASSERT_NE(exception_str.find("remain offsets=99"), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  DeleteTestTupleTableSlot(rslot);
  delete relation;
  delete reader;
}

}  // namespace exceptions

}  // namespace pax::tests
