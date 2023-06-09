#include "comm/paxc_utils.h"

#include "comm/gtest_wrappers.h"

extern int gp_debug_linger;

namespace pax::tests {
#define PAX_TEST_CMD_LENGTH 2048
#define PAX_TEST_LIST_FILE_NUM 128
static const char *pax_copy_test_dir = "/tmp/copytest";
static const char *pax_copy_src_path = "/tmp/test_src";
static const char *pax_copy_dst_path = "/tmp/copytest/test_dst";
static const char *pax_copy_content = "12345678";
static const char *pax_list_path = "/tmp/testlist";
static const char *pax_file_pathname =
    "/tmp/pg_tblspc/16400/GPDB_1_302206171/13261/16394";
static const char *pax_file_pathnull = NULL;
static const char *pax_file_pathempty = "";

class TablePaxUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    gp_debug_linger = 0;
  }

  void TearDown() override {}
};

TEST_F(TablePaxUtilsTest, list_directory) {
  char cmd[PAX_TEST_CMD_LENGTH];
  List *filelist;

  snprintf(cmd, sizeof(cmd), "rm -rf %s", pax_list_path);
  system(cmd);

  snprintf(cmd, sizeof(cmd), "mkdir -p %s", pax_list_path);
  system(cmd);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    snprintf(cmd, sizeof(cmd), "echo '%s' > %s/test%d", pax_copy_content,
             pax_list_path, i);
    system(cmd);
  }

  filelist = paxc::ListDirectory(pax_list_path);
  ASSERT_EQ(list_length(filelist), PAX_TEST_LIST_FILE_NUM);

  ASSERT_DEATH(paxc::ListDirectory(pax_file_pathnull), "");

  ASSERT_DEATH(paxc::ListDirectory(pax_file_pathempty), "");
}

TEST_F(TablePaxUtilsTest, copy_file) {
  int result = 0;
  char cmd[PAX_TEST_CMD_LENGTH];

  snprintf(cmd, sizeof(cmd), "rm -rf %s", pax_copy_test_dir);
  system(cmd);

  snprintf(cmd, sizeof(cmd), "echo '%s' > %s;chmod 600 %s", pax_copy_content,
           pax_copy_src_path, pax_copy_src_path);
  system(cmd);

  paxc::MakedirRecursive(pax_copy_test_dir);

  InitFileAccess();
  paxc::CopyFile(pax_copy_src_path, pax_copy_dst_path);
  result = access(pax_copy_dst_path, F_OK);
  ASSERT_NE(result, -1);

  ASSERT_DEATH(paxc::CopyFile(pax_file_pathnull, pax_file_pathnull), "");

  ASSERT_DEATH(paxc::CopyFile(pax_file_pathempty, pax_file_pathempty), "");
}

TEST_F(TablePaxUtilsTest, makedir_recursive) {
  int result = 0;
  struct stat st;
  char cmd[PAX_TEST_CMD_LENGTH];

  snprintf(cmd, sizeof(cmd), "rm -rf %s", pax_file_pathname);
  system(cmd);

  paxc::MakedirRecursive(pax_file_pathname);
  result = stat(pax_file_pathname, &st);
  ASSERT_EQ(result, 0);

  ASSERT_DEATH(paxc::MakedirRecursive(pax_file_pathnull), "");

  ASSERT_DEATH(paxc::MakedirRecursive(pax_file_pathempty), "");
}

}  // namespace pax::tests
