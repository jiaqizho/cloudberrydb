#include <gtest/gtest.h>

#include "storage/file_system.h"
#include "storage/local_file_system.h"

namespace pax::tests {

class LocalFileSystemTest : public ::testing::Test {
 public:
    void SetUp() override {
        LocalFileSystem::Init();
    }

    void TearDown() override {
        LocalFileSystem::GetInstance()->Delete(file_name);
    }

 protected:
    const std::string file_name = "./test.file";
};

TEST_F(LocalFileSystemTest, open) {
    auto local_fs = LocalFileSystem::GetInstance();
    ASSERT_NE(nullptr, local_fs);

    auto file_ptr = local_fs->Open(file_name);
    EXPECT_NE(nullptr, file_ptr);

    file_ptr->Close();
    delete file_ptr;
}

TEST_F(LocalFileSystemTest, build_path) {
    auto local_fs = LocalFileSystem::GetInstance();
    ASSERT_NE(nullptr, local_fs);

    auto file_ptr = local_fs->Open(file_name);
    ASSERT_NE(nullptr, file_ptr);

    auto path = local_fs->BuildPath(file_ptr);
    ASSERT_EQ(path, "./test.file");

    file_ptr->Close();
    delete file_ptr;
}

TEST_F(LocalFileSystemTest, write_read) {
    auto local_fs = LocalFileSystem::GetInstance();
    ASSERT_NE(nullptr, local_fs);

    auto file_ptr = local_fs->Open(file_name);
    ASSERT_NE(nullptr, file_ptr);

    auto write_size = file_ptr->Write("abc", 3);
    ASSERT_EQ(3, write_size);

    file_ptr->Flush();
    file_ptr->Close();
    file_ptr = local_fs->Open(file_name);
    ASSERT_NE(nullptr, file_ptr);

    char buff[10] = {0};
    auto read_size = file_ptr->Read(buff, 3);
    ASSERT_EQ(3, read_size);
    ASSERT_TRUE(strncmp("abc", buff, 3) == 0); // NOLINT
}

}  // namespace pax::tests
