#include "storage/orc/orc.h"  // NOLINT

#include <cstdio>
#include <random>
#include <string>
#include <utility>

#include "access/tupdesc_details.h"
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/column_read_info.h"
#include "storage/local_file_system.h"

namespace pax::tests {

// 3 clomun - string(len 100), string(len 100), int(len 4)
#define COLUMN_NUMS 3
#define COLUMN_SIZE 100
#define INT32_COLUMN_VALUE 0x123
#define INT32_COLUMN_VALUE_DEFAULT 0x001
#define PROJECTION_COLUMN 2
#define PROJECTION_COLUMN_SINGLE 1

static bool k_proj_noseq[COLUMN_NUMS] =
                              {true, false, true};
static int k_proj_noseq_atts[PROJECTION_COLUMN] =
                              {0, 2};

static bool k_proj_noseq_left[COLUMN_NUMS] =
                              {true, false, false};
static int k_proj_noseq_atts_left[PROJECTION_COLUMN] =
                              {0};

static bool k_proj_noseq_middle[COLUMN_NUMS] =
                              {false, true, false};
static int k_proj_noseq_atts_middle[PROJECTION_COLUMN] =
                              {1};

static bool k_proj_noseq_right[COLUMN_NUMS] =
                              {false, false, true};
static int k_proj_noseq_atts_right[PROJECTION_COLUMN] =
                              {2};

static bool k_proj_seq1[COLUMN_NUMS] =
                              {false, true, true};
static int k_proj_seq_atts1[PROJECTION_COLUMN] =
                              {1, 2};

static bool k_proj_seq2[COLUMN_NUMS] =
                              {true, true, false};
static int k_proj_seq_atts2[PROJECTION_COLUMN] =
                              {0, 1};

static bool k_proj_seq3[COLUMN_NUMS] =
                              {true, true, true};
static int k_proj_seq_atts3[COLUMN_NUMS] =
                              {0, 1, 2};

static void GenFakeBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

class OrcTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance();
    remove(file_name_.c_str());

    MemoryContext orc_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "OrcTestMemoryContext", 80 * 1024 * 1024,
        80 * 1024 * 1024, 80 * 1024 * 1024);

    MemoryContextSwitchTo(orc_test_memory_context);
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "OrcTestResourceOwner");
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ResourceOwner tmp_resource_owner = CurrentResourceOwner;
    CurrentResourceOwner = NULL;
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false,
                         true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_LOCKS, false, true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_AFTER_LOCKS, false,
                         true);
    ResourceOwnerDelete(tmp_resource_owner);
  }

 protected:
  static CTupleSlot *CreateFakeCTupleSlot(bool with_value = true) {
    TupleTableSlot *tuple_slot = nullptr;

    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));

    tuple_desc->natts = COLUMN_NUMS;
    tuple_desc->attrs[0] = {
        .attlen = -1,
        .attbyval = false,
    };

    tuple_desc->attrs[1] = {
        .attlen = -1,
        .attbyval = false,
    };

    tuple_desc->attrs[2] = {
        .attlen = 4,
        .attbyval = true,
    };

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

    if (with_value) {
      char column_buff[COLUMN_SIZE * 2];
      GenFakeBuffer(column_buff, COLUMN_SIZE);
      GenFakeBuffer(column_buff + COLUMN_SIZE, COLUMN_SIZE);

      bool *fake_is_null =
          reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));

      fake_is_null[0] = false;
      fake_is_null[1] = false;
      fake_is_null[2] = false;

      tuple_slot->tts_values[0] =
          cbdb::DatumFromCString(column_buff, COLUMN_SIZE);
      tuple_slot->tts_values[1] =
          cbdb::DatumFromCString(column_buff + COLUMN_SIZE, COLUMN_SIZE);
      tuple_slot->tts_values[2] = cbdb::Int32ToDatum(INT32_COLUMN_VALUE);
      tuple_slot->tts_isnull = fake_is_null;
    }

    auto ctuple_slot = new CTupleSlot(tuple_slot);

    return ctuple_slot;
  }

  static CTupleSlot *CreateEmptyCTupleSlot() {
    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));
    bool *fake_is_null =
        reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
    auto tuple_slot = reinterpret_cast<TupleTableSlot *>(
        cbdb::Palloc0(sizeof(TupleTableSlot)));
    auto tts_values =
        reinterpret_cast<Datum *>(cbdb::Palloc0(sizeof(Datum) * COLUMN_NUMS));
    tuple_desc->natts = COLUMN_NUMS;
    tuple_desc->attrs[0] = {
        .attlen = -1,
        .attbyval = false,
    };

    tuple_desc->attrs[1] = {
        .attlen = -1,
        .attbyval = false,
    };

    tuple_desc->attrs[2] = {
        .attlen = 4,
        .attbyval = true,
    };
    tuple_slot->tts_tupleDescriptor = tuple_desc;
    tuple_slot->tts_values = tts_values;
    tuple_slot->tts_isnull = fake_is_null;
    return new CTupleSlot(tuple_slot);
  }

  static void DeleteCTupleSlot(CTupleSlot *ctuple_slot) {
    auto tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    cbdb::Pfree(tuple_table_slot->tts_tupleDescriptor);
    if (tuple_table_slot->tts_isnull) {
      cbdb::Pfree(tuple_table_slot->tts_isnull);
    }

    cbdb::Pfree(tuple_table_slot);
    delete ctuple_slot;
  }

  static void GenerateTestColumnStats(::orc::proto::StripeStatistics &stripe_statistics,  // NOLINT
                                                             int column_number, bool *hasnull) {
    for (int i = 0; i < column_number; i++) {
      auto pb_stats = stripe_statistics.add_colstats();
      pb_stats->set_hasnull(hasnull[i]);
    }
  }

  static void VerifySingleStripe(PaxColumns *columns, ColumnProjectionInfo *projection_info = nullptr) {
    char column_buff[COLUMN_SIZE];
    struct varlena *vl = nullptr;
    struct varlena *tunpacked = nullptr;
    int read_len = -1;
    char *read_data = nullptr;
    size_t index = 0;

    GenFakeBuffer(column_buff, COLUMN_SIZE);

    if (projection_info)
      EXPECT_TRUE(projection_info->GetProjectionAttsNum() <= COLUMN_NUMS);
    else
      EXPECT_EQ(COLUMN_NUMS, columns->GetColumns());

    if (projection_info &&
        ColumnReadInfo::GetReadColumnIndex(projection_info->GetProjectionAttsArray(),
        projection_info->GetProjectionAttsNum(), index) != PAX_COLUMN_READ_INDEX_NOT_DEFINED) {
      auto column1 = reinterpret_cast<PaxNonFixedColumn *>((*columns)[index++]);
      EXPECT_EQ(1, column1->GetNonNullRows());
      char *column1_buffer = column1->GetBuffer(0).first;
      EXPECT_EQ(0,
                std::memcmp(column1_buffer + VARHDRSZ, column_buff, COLUMN_SIZE));
      vl = (struct varlena *)DatumGetPointer(column1_buffer);
      tunpacked = pg_detoast_datum_packed(vl);
      EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);
      read_len = VARSIZE(tunpacked);
      read_data = VARDATA_ANY(tunpacked);
      // read_len is COLUMN_SIZE + VARHDRSZ
      // because DatumFromCString set it
      EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
      EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
      // read_data should pass the pointer rather than memcpy
      EXPECT_EQ(read_data, column1_buffer + VARHDRSZ);
    }

    if (projection_info &&
        ColumnReadInfo::GetReadColumnIndex(projection_info->GetProjectionAttsArray(),
        projection_info->GetProjectionAttsNum(), index) != PAX_COLUMN_READ_INDEX_NOT_DEFINED) {
      auto column2 = reinterpret_cast<PaxNonFixedColumn *>((*columns)[index++]);
      char *column2_buffer = column2->GetBuffer(0).first;
      EXPECT_EQ(1, column2->GetNonNullRows());
      EXPECT_EQ(0,
                std::memcmp(column2_buffer + VARHDRSZ, column_buff, COLUMN_SIZE));
      vl = (struct varlena *)DatumGetPointer(column2_buffer);
      tunpacked = pg_detoast_datum_packed(vl);
      EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);
      read_len = VARSIZE(tunpacked);
      read_data = VARDATA_ANY(tunpacked);
      EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
      EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
      EXPECT_EQ(read_data, column2_buffer + VARHDRSZ);
    }

    if (projection_info &&
        ColumnReadInfo::GetReadColumnIndex(projection_info->GetProjectionAttsArray(),
        projection_info->GetProjectionAttsNum(), index) != PAX_COLUMN_READ_INDEX_NOT_DEFINED) {
      auto column3 = reinterpret_cast<PaxCommColumn<int32> *>((*columns)[index++]);
      auto column3_buffer = column3->GetDataBuffer();
      EXPECT_EQ(1, column3_buffer->GetSize());
      EXPECT_EQ(INT32_COLUMN_VALUE, (*column3_buffer)[0]);
    }
  }

 protected:
  const std::string file_name_ = "./test.file";
};

TEST_F(OrcTest, WriteTuple) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  DeleteCTupleSlot(tuple_slot);
  delete writer;
}

TEST_F(OrcTest, OpenOrc) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;
  auto writer = new OrcWriter(writer_options, std::move(types), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  reader->GetStripeInfo(0);
  reader->Close();

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadStripes) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  // file_ptr in orc writer will be freed when writer do destruct
  // current OrcWriter::CreateWriter only for test
  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  // file_ptr in orc reader will be freed when reader do destruct
  // should not direct used OrcReader in TableReader(use OrcIteratorReader
  // instead)
  //
  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  auto columns = reader->ReadStripe(0);
  OrcTest::VerifySingleStripe(columns);
  reader->Close();

  delete columns;
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadStripesTwice) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;
  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  auto columns_stripe = reader->ReadStripe(0);

  reader->Close();
  char column_buff[COLUMN_SIZE];

  GenFakeBuffer(column_buff, COLUMN_SIZE);

  EXPECT_EQ(COLUMN_NUMS, columns_stripe->GetColumns());
  auto column1 = reinterpret_cast<PaxNonFixedColumn *>((*columns_stripe)[0]);
  auto column2 = reinterpret_cast<PaxNonFixedColumn *>((*columns_stripe)[1]);

  EXPECT_EQ(2, column1->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(0).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(1).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(2, column2->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column2->GetBuffer(0).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column2->GetBuffer(1).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));

  delete columns_stripe;
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadMultiStripes) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));

  EXPECT_EQ(2, reader->GetNumberOfStripes());
  auto columns1 = reader->ReadStripe(0);
  auto columns2 = reader->ReadStripe(1);
  OrcTest::VerifySingleStripe(columns1);
  OrcTest::VerifySingleStripe(columns2);
  reader->Close();

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadCloseEmptyOrc) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);
  writer->WriteTuple(tuple_slot);
  writer->Flush();

  // close without any data
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  auto columns = reader->ReadStripe(0);
  OrcTest::VerifySingleStripe(columns);
  reader->Close();

  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadEmptyOrc) {
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);
  // flush empty
  writer->Flush();
  // direct close
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(0, reader->GetNumberOfStripes());
  reader->Close();

  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTuple) {
  char column_buff[COLUMN_SIZE];

  GenFakeBuffer(column_buff, COLUMN_SIZE);

  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  tuple_slot_empty->GetTupleDesc()->natts = COLUMN_NUMS;
  reader->ReadTuple(tuple_slot_empty);

  auto vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[0]);
  auto tunpacked = pg_detoast_datum_packed(vl);
  EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);

  int read_len = VARSIZE(tunpacked);
  char *read_data = VARDATA_ANY(tunpacked);

  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleDefaultColumn) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto *writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  TupleTableSlot *slot = tuple_slot_empty->GetTupleTableSlot();

  slot->tts_tupleDescriptor->attrs[3] = {
      .attlen = 4,
      .attbyval = true,
  };

  slot->tts_tupleDescriptor->natts = COLUMN_NUMS + 1;

  slot->tts_tupleDescriptor->attrs[3].atthasmissing = true;
  slot->tts_tupleDescriptor->constr =
      reinterpret_cast<TupleConstr *>(cbdb::Palloc0(sizeof(TupleConstr)));
  slot->tts_tupleDescriptor->constr->missing = reinterpret_cast<AttrMissing *>(
      cbdb::Palloc0((COLUMN_NUMS + 1) * sizeof(AttrMissing)));

  slot->tts_tupleDescriptor->constr->missing[3].am_value =
      cbdb::Int32ToDatum(INT32_COLUMN_VALUE_DEFAULT);
  slot->tts_tupleDescriptor->constr->missing[3].am_present = true;
  reader->ReadTuple(tuple_slot_empty);

  ASSERT_EQ(tuple_slot_empty->GetTupleTableSlot()->tts_values[3],
            INT32_COLUMN_VALUE_DEFAULT);

  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleDroppedColumn) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto *writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  TupleTableSlot *slot = tuple_slot_empty->GetTupleTableSlot();

  slot->tts_tupleDescriptor->attrs[2].attisdropped = true;

  reader->ReadTuple(tuple_slot_empty);

  ASSERT_EQ(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2], true);

  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleDroppedColumnWithProjection) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);
  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  TupleTableSlot *slot = tuple_slot_empty->GetTupleTableSlot();

  slot->tts_tupleDescriptor->attrs[2].attisdropped = true;

  reader->ReadTuple(tuple_slot_empty);

  ASSERT_EQ(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2], true);

  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadBigTuple) {
  TupleTableSlot *tuple_slot = nullptr;
  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

  tuple_desc->natts = 2;
  tuple_desc->attrs[0] = {
      .attlen = 4,
      .attbyval = true,
  };
  tuple_desc->attrs[1] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  bool *fake_is_null =
      reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
  fake_is_null[0] = false;
  fake_is_null[1] = false;

  tuple_slot->tts_values[0] = Int32GetDatum(0);
  tuple_slot->tts_values[1] = Int32GetDatum(1);
  tuple_slot->tts_isnull = fake_is_null;
  auto ctuple_slot = new CTupleSlot(tuple_slot);

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (size_t i = 0; i < 10000; i++) {
    ctuple_slot->GetTupleTableSlot()->tts_values[0] = Int32GetDatum(i);
    ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int32GetDatum(i + 1);
    writer->WriteTuple(ctuple_slot);
  }

  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());
  for (size_t i = 0; i < 10000; i++) {
    ASSERT_TRUE(reader->ReadTuple(ctuple_slot));
    ASSERT_EQ(reader->Offset(), i + 1);
    ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[0], i);
    ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[1], i + 1);
  }
  reader->Close();

  DeleteCTupleSlot(ctuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadWithSeek) {
  TupleTableSlot *tuple_slot = nullptr;
  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

  tuple_desc->natts = 2;
  tuple_desc->attrs[0] = {
      .attlen = 4,
      .attbyval = true,
  };
  tuple_desc->attrs[1] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  bool *fake_is_null =
      reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
  fake_is_null[0] = false;
  fake_is_null[1] = false;

  tuple_slot->tts_values[0] = Int32GetDatum(0);
  tuple_slot->tts_values[1] = Int32GetDatum(1);
  tuple_slot->tts_isnull = fake_is_null;
  auto ctuple_slot = new CTupleSlot(tuple_slot);

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (size_t i = 0; i < 10000; i++) {
    ctuple_slot->GetTupleTableSlot()->tts_values[0] = Int32GetDatum(i);
    ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int32GetDatum(i + 1);
    writer->WriteTuple(ctuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  EXPECT_EQ(1, reader->GetNumberOfStripes());

  auto get_rand = [](int l, int r) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(l, r);
    return dist(gen);
  };

  size_t rand_value = 0;

  // rand seek
  for (size_t i = 0; i < 10; i++) {
    rand_value = get_rand(1, 9999);
    reader->Seek(rand_value);
    ASSERT_EQ(reader->Offset(), rand_value);
    ASSERT_TRUE(reader->ReadTuple(ctuple_slot));
    ASSERT_EQ(reader->Offset(), rand_value + 1);
    ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[0], rand_value);
    ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[1], rand_value + 1);
  }

  // bound seek
  reader->Seek(0);
  ASSERT_EQ(reader->Offset(), 0);
  ASSERT_TRUE(reader->ReadTuple(ctuple_slot));
  ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[0], 0);
  ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[1], 1);

  reader->Seek(9999);
  ASSERT_EQ(reader->Offset(), 9999);
  ASSERT_TRUE(reader->ReadTuple(ctuple_slot));
  ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[0], 9999);
  ASSERT_EQ(ctuple_slot->GetTupleTableSlot()->tts_values[1], 10000);

  reader->Seek(10000);
  ASSERT_EQ(reader->Offset(), 10000);
  ASSERT_FALSE(reader->ReadTuple(ctuple_slot));

  bool trigger_exception = false;
  try {
    reader->Seek(10001);
  } catch (cbdb::CException &exception) {
    trigger_exception = true;
    ASSERT_EQ(exception.EType(), cbdb::CException::ExType::kExTypeOutOfRange);
  }
  ASSERT_TRUE(trigger_exception);
  ASSERT_EQ(reader->Offset(), 0);

  reader->Close();

  DeleteCTupleSlot(ctuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadNoFixedColumnInSameTuple) {
  char column_buff_origin[COLUMN_SIZE];
  char column_buff_reset[COLUMN_SIZE];

  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);

  // using the same tuple slot with different data
  cbdb::Pfree(
      cbdb::PointerFromDatum(tuple_slot->GetTupleTableSlot()->tts_values[0]));
  memset(&column_buff_reset, 0, COLUMN_SIZE);
  tuple_slot->GetTupleTableSlot()->tts_values[0] =
      cbdb::DatumFromCString(column_buff_reset, COLUMN_SIZE);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  auto columns = reader->ReadStripe(0);

  EXPECT_EQ(COLUMN_NUMS, columns->GetColumns());
  auto column1 = reinterpret_cast<PaxNonFixedColumn *>((*columns)[0]);

  GenFakeBuffer(column_buff_origin, COLUMN_SIZE);

  EXPECT_EQ(2, column1->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(0).first + VARHDRSZ,
                           column_buff_origin, COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(1).first + VARHDRSZ,
                           column_buff_reset, COLUMN_SIZE));

  reader->Close();

  delete columns;
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, WriteReadWithNullField) {
  char column_buff[COLUMN_SIZE];
  CTupleSlot *ctuple_slot = CreateFakeCTupleSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  GenFakeBuffer(column_buff, COLUMN_SIZE);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  auto *writer = new OrcWriter(writer_options, types, file_ptr);

  // str str int
  // null null int
  // str str null
  // null null null
  writer->WriteTuple(ctuple_slot);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = false;
  writer->WriteTuple(ctuple_slot);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = true;
  writer->WriteTuple(ctuple_slot);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = true;
  writer->WriteTuple(ctuple_slot);

  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  tuple_slot_empty->GetTupleDesc()->natts = COLUMN_NUMS;

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);
  EXPECT_EQ(
      cbdb::DatumToInt32(tuple_slot_empty->GetTupleTableSlot()->tts_values[2]),
      INT32_COLUMN_VALUE);

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);
  auto vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[0]);
  int read_len = VARSIZE(vl);
  char *read_data = VARDATA_ANY(vl);
  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));

  vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[1]);
  read_len = VARSIZE(vl);
  read_data = VARDATA_ANY(vl);
  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);

  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(ctuple_slot);
  delete reader;
  delete writer;
}

TEST_F(OrcTest, WriteReadWithBoundNullField) {
  char column_buff[COLUMN_SIZE];
  CTupleSlot *ctuple_slot = CreateFakeCTupleSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  GenFakeBuffer(column_buff, COLUMN_SIZE);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  auto *writer = new OrcWriter(writer_options, types, file_ptr);

  // null null null
  // str str int
  // null null null
  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = true;
  writer->WriteTuple(ctuple_slot);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = false;
  writer->WriteTuple(ctuple_slot);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = true;
  writer->WriteTuple(ctuple_slot);

  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  tuple_slot_empty->GetTupleDesc()->natts = COLUMN_NUMS;

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);

  auto vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[0]);
  int read_len = VARSIZE(vl);
  char *read_data = VARDATA_ANY(vl);
  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));

  vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[1]);
  read_len = VARSIZE(vl);
  read_data = VARDATA_ANY(vl);
  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
  EXPECT_EQ(DatumGetInt32(tuple_slot_empty->GetTupleTableSlot()->tts_values[2]),
            INT32_COLUMN_VALUE);

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);

  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(ctuple_slot);
  delete reader;
  delete writer;
}

TEST_F(OrcTest, WriteReadWithALLNullField) {
  CTupleSlot *ctuple_slot = CreateFakeCTupleSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto *file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  auto *writer = new OrcWriter(writer_options, types, file_ptr);

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[2] = true;
  for (size_t i = 0; i < 1000; i++) {
    writer->WriteTuple(ctuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto *reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  EXPECT_EQ(1, reader->GetNumberOfStripes());
  tuple_slot_empty->GetTupleDesc()->natts = COLUMN_NUMS;
  for (size_t i = 0; i < 1000; i++) {
    reader->ReadTuple(tuple_slot_empty);
    EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[0]);
    EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[1]);
    EXPECT_TRUE(tuple_slot_empty->GetTupleTableSlot()->tts_isnull[2]);
  }
  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(ctuple_slot);
  delete reader;
  delete writer;
}

TEST_F(OrcTest, ReadTupleWithProjectionColumn) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_noseq);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_noseq, COLUMN_NUMS);
  ASSERT_TRUE(column_read_info.size() == PROJECTION_COLUMN);
  auto iter = column_read_info.begin();
  for (int i = 0; i < PROJECTION_COLUMN; i++) { // NOLINT
    ASSERT_EQ(iter->first, k_proj_noseq_atts[i]);
    ASSERT_EQ(iter->second, k_proj_noseq_atts[i]);
    iter++;
  }

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_noseq_atts[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleWithProjectionColumnSingleLeft) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_noseq_left);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_noseq_left, COLUMN_NUMS);
  ASSERT_TRUE(column_read_info.size() == PROJECTION_COLUMN_SINGLE);
  auto iter = column_read_info.begin();
  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    ASSERT_EQ(iter->first, k_proj_noseq_atts_left[i]);
    ASSERT_EQ(iter->second, k_proj_noseq_atts_left[i]);
    iter++;
  }

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_noseq_atts_left[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleWithProjectionColumnSingleMiddle) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_noseq_middle);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_noseq_middle, COLUMN_NUMS);
  ASSERT_TRUE(column_read_info.size() == PROJECTION_COLUMN_SINGLE);
  auto iter = column_read_info.begin();
  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    ASSERT_EQ(iter->first, k_proj_noseq_atts_middle[i]);
    ASSERT_EQ(iter->second, k_proj_noseq_atts_middle[i]);
    iter++;
  }

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_noseq_atts_middle[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleWithProjectionColumnSingleRight) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_noseq_right);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_noseq_right, COLUMN_NUMS);
  ASSERT_TRUE(column_read_info.size() == PROJECTION_COLUMN_SINGLE);
  auto iter = column_read_info.begin();
  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    ASSERT_EQ(iter->first, k_proj_noseq_atts_right[i]);
    ASSERT_EQ(iter->second, k_proj_noseq_atts_right[i]);
    iter++;
  }

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN_SINGLE; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_noseq_atts_right[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}


TEST_F(OrcTest, ReadTupleWithSeqProjectionColumn1) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_seq1);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq1, COLUMN_NUMS);
  auto iter = column_read_info.begin();

  ASSERT_EQ(iter->first, k_proj_seq_atts1[0]);
  ASSERT_EQ(iter->second, k_proj_seq_atts1[1]);

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_seq_atts1[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleWithSeqProjectionColumn2) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_seq2);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq2, COLUMN_NUMS);
  auto iter = column_read_info.begin();

  ASSERT_EQ(iter->first, k_proj_seq_atts2[0]);
  ASSERT_EQ(iter->second, k_proj_seq_atts2[1]);

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < PROJECTION_COLUMN; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_seq_atts2[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(OrcTest, ReadTupleWithSeqProjectionColumn3) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_);

  auto reader =
      reinterpret_cast<OrcReader *>(OrcReader::CreateReader(file_ptr));
  pax::MicroPartitionReader::ReaderOptions options;
  reader->Open(options);

  EXPECT_EQ(2, reader->GetNumberOfStripes());

  auto projection_info =
          new pax::ColumnProjectionInfo(COLUMN_NUMS, k_proj_seq3);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq3, COLUMN_NUMS);
  auto iter = column_read_info.begin();

  ASSERT_EQ(iter->first, k_proj_seq_atts3[0]);
  ASSERT_EQ(iter->second, k_proj_seq_atts3[2]);

  auto columns1 = reader->ReadStripe(0, column_read_info);
  auto columns2 = reader->ReadStripe(1, column_read_info);
  OrcTest::VerifySingleStripe(columns1, projection_info);
  OrcTest::VerifySingleStripe(columns2, projection_info);
  reader->Close();

  for (int i = 0; i < COLUMN_NUMS; i++) {
    int idx = projection_info->GetProjectionAtts(i);
    ASSERT_EQ(idx, k_proj_seq_atts3[i]);
  }

  delete columns1;
  delete columns2;

  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}
}  // namespace pax::tests
