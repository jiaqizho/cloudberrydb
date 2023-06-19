#include "storage/file_system.h"

#include <gtest/gtest.h>

#include "comm/singleton.h"
#include "storage/local_file_system.h"

namespace pax::tests {

class LocalFileSystemTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {
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

}  // namespace pax::tests
