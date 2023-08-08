#include "storage/orc/orc.h"
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "access/tupdesc_details.h"
#include "storage/column_read_info.h"
#include "storage/column_projection_info.h"

#include <cstdio>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace pax::tests {
#define PROJECTION_ATTS 8
#define PROJECTION_NOSEQ_COLUMN 3
#define PROJECTION_SEQ_COLUMN 5
#define PROJECTION_SEQ_BOUND_COLUMN 4
#define PROJECTION_SINGLE_COLUMN 1

static bool k_proj_noseq[PROJECTION_ATTS] =
                              {false, true, false, true, false, false, true, false};
static int k_proj_noseq_atts[PROJECTION_NOSEQ_COLUMN] =
                              {1, 3, 6};
static std::vector<std::pair<int, int>> k_proj_noseq_readinfo =
                              { {1, 1}, {3, 3}, {6, 6} };

static bool k_proj_seq[PROJECTION_ATTS] =
                              {false, true, true, true, false, true, true, false};
static int k_proj_seq_atts[PROJECTION_SEQ_COLUMN] =
                              {1, 2, 3, 5, 6};
static std::vector<std::pair<int, int>> k_proj_seq_readinfo =
                             { {1, 3}, {5, 6} };

static bool k_proj_seq_left[PROJECTION_ATTS] =
                              {true, true, true, true, false, false, false, false};
static int k_proj_seq_left_atts[PROJECTION_SEQ_COLUMN] =
                              {0, 1, 2, 3};
static std::vector<std::pair<int, int>> k_proj_seq_left_readinfo =
                              { {0, 3} };

static bool k_proj_seq_right[PROJECTION_ATTS] =
                              {false, false, false, false, true, true, true, true};
static int k_proj_seq_right_atts[PROJECTION_SEQ_BOUND_COLUMN] =
                              {4, 5, 6, 7};
static std::vector<std::pair<int, int>> k_proj_seq_right_readinfo =
                              { {4, 7} };

static bool k_proj_seq_all[PROJECTION_ATTS] =
                              {true, true, true, true, true, true, true, true};
static std::vector<std::pair<int, int>> k_proj_seq_all_readinfo =
                              { {0, 7} };

static bool k_proj_seq_single_left[PROJECTION_ATTS] =
                              {true, false, false, false, false, false, false, false};
static std::vector<std::pair<int, int>> k_proj_seq_single_left_readinfo =
                              { {0, 0} };

static bool k_proj_seq_single_right[PROJECTION_ATTS] =
                              {false, false, false, false, false, false, false, true};
static std::vector<std::pair<int, int>> k_proj_seq_single_right_readinfo =
                              { {7, 7} };

static bool k_proj_seq_single_middle[PROJECTION_ATTS] =
                              {false, false, false, false, true, false, false, false};
static std::vector<std::pair<int, int>> k_proj_seq_single_middle_readinfo =
                              { {4, 4} };

class ColumnProjectionInfoTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

// No-sequential projection info test
TEST_F(ColumnProjectionInfoTest, ProjectionInfoNoseqColumn) {
  auto projection_info =
              new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_noseq);

  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_NOSEQ_COLUMN);
  for (int i = 0; i < PROJECTION_NOSEQ_COLUMN; i++)
    ASSERT_EQ(projection_info->GetProjectionAtts(i), k_proj_noseq_atts[i]);

  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_noseq, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_noseq_readinfo.begin(); iter_result != k_proj_noseq_readinfo.end(); iter_result++) {
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

// Sequential projection info test
TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqColumn) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq);

  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_SEQ_COLUMN);
  for (int i = 0; i < PROJECTION_SEQ_COLUMN; i++)
    ASSERT_EQ(projection_info->GetProjectionAtts(i), k_proj_seq_atts[i]);

  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_readinfo.begin(); iter_result != k_proj_seq_readinfo.end(); iter_result++) {
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqLboundColumn) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_left);

  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_SEQ_BOUND_COLUMN);
  for (int i = 0; i < PROJECTION_SEQ_BOUND_COLUMN; i++)
    ASSERT_EQ(projection_info->GetProjectionAtts(i), k_proj_seq_left_atts[i]);

  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_left, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_left_readinfo.begin(); iter_result != k_proj_seq_left_readinfo.end(); iter_result++) {  //NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

// Sequential projection info with right bound limit test
TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqRboundColumn) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_right);

  for (int i = 0; i < PROJECTION_SEQ_BOUND_COLUMN; i++)
    ASSERT_EQ(projection_info->GetProjectionAtts(i), k_proj_seq_right_atts[i]);

  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_right, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_right_readinfo.begin(); iter_result != k_proj_seq_right_readinfo.end(); iter_result++) {  //NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

// Sequential projection info with all column test
TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqAllColumn) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_all);

  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_ATTS);

  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_all, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_all_readinfo.begin(); iter_result != k_proj_seq_all_readinfo.end(); iter_result++) {  // NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

// Sequential projection info with single column test
TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqSingleColumnLeft) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_single_left);
  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_SINGLE_COLUMN);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_single_left, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_single_left_readinfo.begin(); iter_result != k_proj_seq_single_left_readinfo.end(); iter_result++) { //NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqSingleColumnRight) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_single_right);
  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_SINGLE_COLUMN);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_single_right, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_single_right_readinfo.begin(); iter_result != k_proj_seq_single_right_readinfo.end(); iter_result++) { //NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

TEST_F(ColumnProjectionInfoTest, ProjectionInfoSeqSingleColumnMiddle) {
  auto projection_info =
            new pax::ColumnProjectionInfo(PROJECTION_ATTS, k_proj_seq_single_middle);
  ASSERT_EQ(projection_info->GetProjectionAttsNum(), PROJECTION_SINGLE_COLUMN);
  std::vector<std::pair<int, int>> column_read_info =
  ColumnReadInfo::BuildupColumnReadInfo(k_proj_seq_single_middle, PROJECTION_ATTS);
  auto iter_test = column_read_info.begin();
  for (auto iter_result =  k_proj_seq_single_middle_readinfo.begin(); iter_result != k_proj_seq_single_middle_readinfo.end(); iter_result++) { //NOLINT
    ASSERT_EQ(iter_test->first, iter_result->first);
    ASSERT_EQ(iter_test->second, iter_result->second);
    iter_test++;
  }
}

}  // namespace pax::tests

