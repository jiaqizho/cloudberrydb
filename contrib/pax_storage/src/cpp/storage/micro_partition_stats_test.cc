#include "storage/micro_partition_stats.h"

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "pax_gtest_helper.h"
namespace pax::tests {

class MicroPartitionStatsTest : public ::testing::Test {
 public:
  void SetUp() override {
    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override { ReleaseTestResourceOwner(); }
};

TEST_F(MicroPartitionStatsTest, MicroPartitionStatsInfoCombine) {
  stats::MicroPartitionStatisticsInfo mp_stats_info1;
  stats::MicroPartitionStatisticsInfo mp_stats_info2;

  // define columns
  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 4));

  tuple_desc->natts = 4;
  tuple_desc->attrs[0] = {.atttypid = INT4OID,
                          .attlen = 4,
                          .attbyval = true,
                          .attalign = TYPALIGN_INT,
                          .attisdropped = false,
                          .attcollation = InvalidOid};

  tuple_desc->attrs[1] = {.atttypid = TEXTOID,
                          .attlen = -1,
                          .attbyval = false,
                          .attalign = TYPALIGN_DOUBLE,
                          .attisdropped = false,
                          .attcollation = DEFAULT_COLLATION_OID};

  tuple_desc->attrs[2] = {.atttypid = INT4OID,
                          .attlen = 4,
                          .attbyval = true,
                          .attalign = TYPALIGN_INT,
                          .attisdropped = false,
                          .attcollation = InvalidOid};
  tuple_desc->attrs[3] = {.atttypid = INT4OID,
                          .attlen = 4,
                          .attbyval = true,
                          .attalign = TYPALIGN_INT,
                          .attisdropped = false,
                          .attcollation = InvalidOid};

  auto col_stats1_1 = mp_stats_info1.add_columnstats();
  auto col_stats1_2 = mp_stats_info1.add_columnstats();
  auto col_stats1_3 = mp_stats_info1.add_columnstats();
  auto col_stats1_4 = mp_stats_info1.add_columnstats();

  auto col_stats2_1 = mp_stats_info2.add_columnstats();
  auto col_stats2_2 = mp_stats_info2.add_columnstats();
  auto col_stats2_3 = mp_stats_info2.add_columnstats();
  auto col_stats2_4 = mp_stats_info2.add_columnstats();

  // filling allnull and hasnull
  col_stats1_1->set_allnull(true);
  col_stats1_1->set_hasnull(true);

  col_stats1_2->set_allnull(false);
  col_stats1_2->set_hasnull(true);

  col_stats1_3->set_allnull(false);
  col_stats1_3->set_hasnull(true);

  col_stats1_4->set_allnull(false);
  col_stats1_4->set_hasnull(false);

  col_stats2_1->set_allnull(false);
  col_stats2_1->set_hasnull(false);

  col_stats2_2->set_allnull(false);
  col_stats2_2->set_hasnull(true);

  col_stats2_3->set_allnull(false);
  col_stats2_3->set_hasnull(true);

  col_stats2_4->set_allnull(true);
  col_stats2_4->set_hasnull(true);

  // fill ColumnBasicInfo
  col_stats1_1->mutable_info()->set_typid(tuple_desc->attrs[0].atttypid);
  col_stats1_1->mutable_info()->set_opfamily(0);
  col_stats1_1->mutable_info()->set_collation(
      tuple_desc->attrs[0].attcollation);

  col_stats2_1->mutable_info()->CopyFrom(col_stats1_1->info());

  col_stats1_2->mutable_info()->set_typid(tuple_desc->attrs[1].atttypid);
  col_stats1_2->mutable_info()->set_opfamily(0);
  col_stats1_2->mutable_info()->set_collation(
      tuple_desc->attrs[1].attcollation);

  col_stats2_2->mutable_info()->CopyFrom(col_stats1_2->info());

  col_stats1_3->mutable_info()->set_typid(tuple_desc->attrs[2].atttypid);
  col_stats1_3->mutable_info()->set_opfamily(0);
  col_stats1_3->mutable_info()->set_collation(
      tuple_desc->attrs[2].attcollation);

  col_stats2_3->mutable_info()->CopyFrom(col_stats1_3->info());

  col_stats1_4->mutable_info()->set_typid(tuple_desc->attrs[3].atttypid);
  col_stats1_4->mutable_info()->set_opfamily(0);
  col_stats1_4->mutable_info()->set_collation(
      tuple_desc->attrs[3].attcollation);

  col_stats2_4->mutable_info()->CopyFrom(col_stats1_4->info());

  // fill ColumnDataStats
  ASSERT_FALSE(col_stats1_1->mutable_datastats()->has_maximum());
  ASSERT_FALSE(col_stats1_1->mutable_datastats()->has_minimal());

  col_stats1_2->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::DatumFromCString("111", 3), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval));
  col_stats1_2->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::DatumFromCString("333", 3), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval));

  col_stats1_3->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(50), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval));
  col_stats1_3->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(100), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval));

  col_stats1_4->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(50), tuple_desc->attrs[3].attlen,
      tuple_desc->attrs[3].attbyval));
  col_stats1_4->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(100), tuple_desc->attrs[3].attlen,
      tuple_desc->attrs[3].attbyval));

  col_stats2_1->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(50), tuple_desc->attrs[0].attlen,
      tuple_desc->attrs[0].attbyval));
  col_stats2_1->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(100), tuple_desc->attrs[0].attlen,
      tuple_desc->attrs[0].attbyval));

  col_stats2_2->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::DatumFromCString("222", 3), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval));
  col_stats2_2->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::DatumFromCString("444", 3), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval));

  col_stats2_3->mutable_datastats()->set_minimal(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(10), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval));
  col_stats2_3->mutable_datastats()->set_maximum(MicroPartitionStats::ToValue(
      cbdb::Int32ToDatum(60), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval));

  ASSERT_FALSE(col_stats2_4->mutable_datastats()->has_maximum());
  ASSERT_FALSE(col_stats2_4->mutable_datastats()->has_minimal());

  ASSERT_TRUE(MicroPartitionStats::MicroPartitionStatisticsInfoCombine(
      &mp_stats_info1, &mp_stats_info2, tuple_desc, false));

  ASSERT_EQ(mp_stats_info1.columnstats_size(), 4);
  col_stats1_1 = mp_stats_info1.mutable_columnstats(0);
  col_stats1_2 = mp_stats_info1.mutable_columnstats(1);
  col_stats1_3 = mp_stats_info1.mutable_columnstats(2);
  col_stats1_4 = mp_stats_info1.mutable_columnstats(3);

  // verify allnull and hasnull
  ASSERT_FALSE(col_stats1_1->allnull());
  ASSERT_TRUE(col_stats1_1->hasnull());
  ASSERT_FALSE(col_stats1_2->allnull());
  ASSERT_TRUE(col_stats1_2->hasnull());
  ASSERT_FALSE(col_stats1_3->allnull());
  ASSERT_TRUE(col_stats1_3->hasnull());
  ASSERT_FALSE(col_stats1_4->allnull());
  ASSERT_TRUE(col_stats1_4->hasnull());

  // verify ColumnBasicInfo have not been updated
  ASSERT_EQ(col_stats1_1->mutable_info()->typid(),
            tuple_desc->attrs[0].atttypid);
  ASSERT_EQ(col_stats1_1->mutable_info()->opfamily(), (uint32)0);
  ASSERT_EQ(col_stats1_1->mutable_info()->collation(),
            tuple_desc->attrs[0].attcollation);

  ASSERT_EQ(col_stats1_2->mutable_info()->typid(),
            tuple_desc->attrs[1].atttypid);
  ASSERT_EQ(col_stats1_2->mutable_info()->opfamily(), (uint32)0);
  ASSERT_EQ(col_stats1_2->mutable_info()->collation(),
            tuple_desc->attrs[1].attcollation);

  ASSERT_EQ(col_stats1_3->mutable_info()->typid(),
            tuple_desc->attrs[2].atttypid);
  ASSERT_EQ(col_stats1_3->mutable_info()->opfamily(), (uint32)0);
  ASSERT_EQ(col_stats1_3->mutable_info()->collation(),
            tuple_desc->attrs[2].attcollation);

  ASSERT_EQ(col_stats1_4->mutable_info()->typid(),
            tuple_desc->attrs[3].atttypid);
  ASSERT_EQ(col_stats1_4->mutable_info()->opfamily(), (uint32)0);
  ASSERT_EQ(col_stats1_4->mutable_info()->collation(),
            tuple_desc->attrs[3].attcollation);

  // verify ColumnDataStats have been updated
  Datum max_datum = 0;
  Datum min_datum = 0;
  bool ok = false;

  struct varlena *vl, *tunpacked;
  int text_len;
  char *text_data;

  ASSERT_TRUE(col_stats1_1->mutable_datastats()->has_maximum());
  ASSERT_TRUE(col_stats1_1->mutable_datastats()->has_minimal());
  ASSERT_TRUE(col_stats1_2->mutable_datastats()->has_maximum());
  ASSERT_TRUE(col_stats1_2->mutable_datastats()->has_minimal());
  ASSERT_TRUE(col_stats1_3->mutable_datastats()->has_maximum());
  ASSERT_TRUE(col_stats1_3->mutable_datastats()->has_minimal());
  ASSERT_TRUE(col_stats1_4->mutable_datastats()->has_maximum());
  ASSERT_TRUE(col_stats1_4->mutable_datastats()->has_minimal());

  max_datum = MicroPartitionStats::FromValue(
      col_stats1_1->mutable_datastats()->maximum(), tuple_desc->attrs[0].attlen,
      tuple_desc->attrs[0].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(max_datum, cbdb::Int32ToDatum(100));
  min_datum = MicroPartitionStats::FromValue(
      col_stats1_1->mutable_datastats()->minimal(), tuple_desc->attrs[0].attlen,
      tuple_desc->attrs[0].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(min_datum, cbdb::Int32ToDatum(50));

  max_datum = MicroPartitionStats::FromValue(
      col_stats1_2->mutable_datastats()->maximum(), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval, &ok);
  ASSERT_TRUE(ok);
  vl = (struct varlena *)DatumGetPointer(max_datum);
  tunpacked = pg_detoast_datum_packed(vl);
  ASSERT_EQ((Pointer)vl, (Pointer)tunpacked);
  text_len = VARSIZE_ANY_EXHDR(tunpacked);
  text_data = VARDATA_ANY(tunpacked);
  ASSERT_EQ(text_len, 3);
  ASSERT_EQ(memcmp(text_data, "444", 3), 0);

  min_datum = MicroPartitionStats::FromValue(
      col_stats1_2->mutable_datastats()->minimal(), tuple_desc->attrs[1].attlen,
      tuple_desc->attrs[1].attbyval, &ok);
  ASSERT_TRUE(ok);
  vl = (struct varlena *)DatumGetPointer(min_datum);
  tunpacked = pg_detoast_datum_packed(vl);
  ASSERT_EQ((Pointer)vl, (Pointer)tunpacked);
  text_len = VARSIZE_ANY_EXHDR(tunpacked);
  text_data = VARDATA_ANY(tunpacked);
  ASSERT_EQ(text_len, 3);
  ASSERT_EQ(memcmp(text_data, "111", 3), 0);

  max_datum = MicroPartitionStats::FromValue(
      col_stats1_3->mutable_datastats()->maximum(), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(max_datum, cbdb::Int32ToDatum(100));
  min_datum = MicroPartitionStats::FromValue(
      col_stats1_3->mutable_datastats()->minimal(), tuple_desc->attrs[2].attlen,
      tuple_desc->attrs[2].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(min_datum, cbdb::Int32ToDatum(10));

  max_datum = MicroPartitionStats::FromValue(
      col_stats1_4->mutable_datastats()->maximum(), tuple_desc->attrs[3].attlen,
      tuple_desc->attrs[3].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(max_datum, cbdb::Int32ToDatum(100));
  min_datum = MicroPartitionStats::FromValue(
      col_stats1_4->mutable_datastats()->minimal(), tuple_desc->attrs[3].attlen,
      tuple_desc->attrs[3].attbyval, &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(min_datum, cbdb::Int32ToDatum(50));
}

}  // namespace pax::tests