#include "catalog/table_metadata.h"

#include "comm/gtest_wrappers.h"

namespace pax::tests {
class TableMetadataTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};


TEST_F(TableMetadataTest, new_iterator) {
  std::string block_id = "block_id";
  std::string file_name = "file_name";
  std::shared_ptr<std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>>
      micro_partitions = std::make_shared<
          std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>>();

  std::shared_ptr<pax::MicroPartitionMetadata> meta_info_ptr =
      std::make_shared<pax::MicroPartitionMetadata>(block_id, file_name);

  meta_info_ptr->setTupleCount(1);
  meta_info_ptr->setFileSize(100);

  micro_partitions->push_back(meta_info_ptr);

  std::shared_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(micro_partitions);

  ASSERT_TRUE(iterator->HasNext());

  std::shared_ptr<pax::MicroPartitionMetadata> meta_info = iterator->Next();
  ASSERT_EQ(meta_info->getFileName(), file_name);
  ASSERT_EQ(meta_info->getTupleCount(), 1);
  ASSERT_EQ(meta_info->getFileSize(), 100);
}

}  // namespace pax::tests
