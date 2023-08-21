#include <gtest/gtest.h>

#include "catalog/table_metadata.h"
#include <utility>

namespace pax::tests {
#define MPARTITION_FILE_SIZE 100

class TableMetadataTest : public ::testing::Test {
 public:
};

TEST_F(TableMetadataTest, NewIterator) {
  std::string block_id = "block_id";
  std::string file_name = "file_name";
  std::vector<pax::MicroPartitionMetadata> micro_partitions;

  pax::MicroPartitionMetadata meta_info(block_id, file_name);

  meta_info.SetTupleCount(1);
  meta_info.SetFileSize(MPARTITION_FILE_SIZE);

  micro_partitions.push_back(std::move(meta_info));

  std::unique_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(std::move(micro_partitions));

  ASSERT_TRUE(iterator->HasNext());

  pax::MicroPartitionMetadata meta_info_cur = iterator->Next();
  ASSERT_EQ(meta_info_cur.GetFileName(), file_name);
  ASSERT_EQ(meta_info_cur.GetTupleCount(), 1);
  ASSERT_EQ(meta_info_cur.GetFileSize(), MPARTITION_FILE_SIZE);
}

}  // namespace pax::tests
