#include <gtest/gtest.h>

#include "comm/singleton.h"
#include "storage/local_file_system.h"

extern int gp_debug_linger;

namespace pax::tests {
#define PAX_TEST_CMD_LENGTH 2048
#define PAX_TEST_LIST_FILE_NUM 128
static const char* pax_copy_test_dir = "/tmp/copytest";
static const char* pax_copy_src_path = "/tmp/test_src";
static const char* pax_copy_dst_path = "/tmp/copytest/test_dst";
static const char* pax_copy_content = "12345678";
static const char* pax_list_path = "/tmp/testlist";
static const char* pax_file_pathname = "/tmp/pg_tblspc/16400/GPDB_1_302206171/13261/16394";

class LocalFileSystemTest : public ::testing::Test {
 public:
  void SetUp() override {
      gp_debug_linger = 0;
  }

  void TearDown() override {
    gp_debug_linger = 30;
    struct stat st{};
    if (!stat(file_name_.c_str(), &st))
      pax::Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
  }

 protected:
  const std::string file_name_ = "./test.file";
};

TEST_F(LocalFileSystemTest, Open) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  EXPECT_NE(nullptr, file_ptr);

  file_ptr->Close();
  delete file_ptr;
}

TEST_F(LocalFileSystemTest, BuildPath) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  ASSERT_NE(nullptr, file_ptr);

  auto path = local_fs->BuildPath(file_ptr);
  ASSERT_EQ(path, "./test.file");

  file_ptr->Close();
  delete file_ptr;
}

TEST_F(LocalFileSystemTest, WriteRead) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_);
  ASSERT_NE(nullptr, file_ptr);

  auto write_size = file_ptr->Write("abc", 3);
  ASSERT_EQ(3, write_size);

  file_ptr->Flush();
  file_ptr->Close();
  file_ptr = local_fs->Open(file_name_);
  ASSERT_NE(nullptr, file_ptr);

  char buff[10] = {0};
  auto read_size = file_ptr->Read(buff, 3);
  ASSERT_EQ(3, read_size);
  ASSERT_EQ(strncmp("abc", buff, 3), 0);
}

TEST_F(LocalFileSystemTest, ListDirectory) {
  char cmd[PAX_TEST_CMD_LENGTH];
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(pax_list_path, true);
  ASSERT_NE(access(pax_list_path, F_OK), 0);

  fs->CreateDirectory(pax_list_path);
  ASSERT_EQ(access(pax_list_path, F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    snprintf(cmd, sizeof(cmd), "echo '%s' > %s/test%d", pax_copy_content, pax_list_path, i);
    system(cmd); // NOLINT
  }

  filelist = fs->ListDirectory(pax_list_path);
  ASSERT_EQ(filelist.size(), PAX_TEST_LIST_FILE_NUM);
}

TEST_F(LocalFileSystemTest, CopyFile) {
  int result = 0;
  char cmd[PAX_TEST_CMD_LENGTH];
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  fs->DeleteDirectory(pax_copy_test_dir, true);
  ASSERT_NE(access(pax_copy_test_dir, F_OK), 0);

  snprintf(cmd, sizeof(cmd), "echo '%s' > %s;chmod 600 %s", pax_copy_content,
                pax_copy_src_path, pax_copy_src_path);
  system(cmd); // NOLINT

  cbdb::MakedirRecursive(pax_copy_test_dir);

  InitFileAccess();
  fs->CopyFile(pax_copy_src_path, pax_copy_dst_path);
  result = access(pax_copy_dst_path, F_OK);
  ASSERT_NE(result, -1);
}

TEST_F(LocalFileSystemTest, MakedirRecursive) {
  int result = 0;
  struct stat st{};
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  fs->DeleteDirectory(pax_file_pathname, true);
  ASSERT_NE(access(pax_file_pathname, F_OK), 0);

  cbdb::MakedirRecursive(pax_file_pathname);
  result = stat(pax_file_pathname, &st);
  ASSERT_EQ(result, 0);
}

TEST_F(LocalFileSystemTest, CreateDeleteDirectory) {
  char cmd[PAX_TEST_CMD_LENGTH];
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(pax_list_path, true);
  ASSERT_NE(access(pax_list_path, F_OK), 0);

  fs->CreateDirectory(pax_list_path);
  ASSERT_EQ(access(pax_list_path, F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    snprintf(cmd, sizeof(cmd), "echo '%s' > %s/test%d", pax_copy_content, pax_list_path, i);
    system(cmd); // NOLINT
  }

  filelist = fs->ListDirectory(pax_list_path);
  ASSERT_EQ(filelist.size(), PAX_TEST_LIST_FILE_NUM);

  fs->DeleteDirectory(pax_list_path, true);
  ASSERT_NE(access(pax_list_path, F_OK), 0);
}

TEST_F(LocalFileSystemTest, DeleteDirectoryReserveToplevel) {
  char cmd[PAX_TEST_CMD_LENGTH];
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(pax_list_path, true);
  ASSERT_NE(access(pax_list_path, F_OK), 0);

  fs->CreateDirectory(pax_list_path);
  ASSERT_EQ(access(pax_list_path, F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    snprintf(cmd, sizeof(cmd), "echo '%s' > %s/test%d", pax_copy_content, pax_list_path, i);
    system(cmd); // NOLINT
  }

  filelist = fs->ListDirectory(pax_list_path);
  ASSERT_EQ(filelist.size(), PAX_TEST_LIST_FILE_NUM);

  fs->DeleteDirectory(pax_list_path, false);
  ASSERT_EQ(access(pax_list_path, F_OK), 0);

  filelist = fs->ListDirectory(pax_list_path);
  ASSERT_EQ(filelist.size(), 0);
}
}  // namespace pax::tests
