#include "catalog/table_metadata.h"

#include "comm/gtest_wrappers.h"

namespace pax::tests {
#define MPARTITION_FILE_SIZE 100
#define MPARTITION_FILE_NUM 100
#define SEEK_BEGIN_POS 0
#define SEEK_OFFSET_POS 10
#define SEEK_OFFSET_NEG -10
#define SEEK_OFFSET_EXCEED_POS 99
#define SEEK_OFFSET_EXCEED_NEG -99

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
  meta_info_ptr->setFileSize(MPARTITION_FILE_SIZE);

  micro_partitions->push_back(meta_info_ptr);

  std::shared_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(micro_partitions);

  ASSERT_TRUE(iterator->HasNext());

  std::shared_ptr<pax::MicroPartitionMetadata> meta_info = iterator->Next();
  ASSERT_EQ(meta_info->getFileName(), file_name);
  ASSERT_EQ(meta_info->getTupleCount(), 1);
  ASSERT_EQ(meta_info->getFileSize(), MPARTITION_FILE_SIZE);
}

TEST_F(TableMetadataTest, seek_iterator) {
  std::string block_id = "block_id";
  std::string file_name = "file_name";
  std::shared_ptr<std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>>
      micro_partitions = std::make_shared<
          std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>>();

  for (int i = 0; i < MPARTITION_FILE_NUM; i++) {
    std::shared_ptr<pax::MicroPartitionMetadata> meta_info_ptr =
        std::make_shared<pax::MicroPartitionMetadata>(block_id, file_name);
    meta_info_ptr->setTupleCount(1);
    meta_info_ptr->setFileSize(MPARTITION_FILE_SIZE);
    micro_partitions->push_back(meta_info_ptr);
  }

  std::shared_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(micro_partitions);

  ASSERT_TRUE(iterator->HasNext());
  ASSERT_EQ(micro_partitions->size(), MPARTITION_FILE_NUM);


  // *SEEK ITER_SEEK_POS_BEGIN  cases *
  // case seek from begin
  iterator->Seek(SEEK_BEGIN_POS, pax::ITER_SEEK_POS_BEGIN);
  ASSERT_EQ(iterator->getCurrentIndex(), SEEK_BEGIN_POS);

  // case seek from begin exceed max bound.
  ASSERT_DEATH(iterator->Seek(SEEK_OFFSET_EXCEED_POS*10, pax::ITER_SEEK_POS_BEGIN), "");

  // case seek from begin exceed min bound.
  ASSERT_DEATH(iterator->Seek(SEEK_OFFSET_EXCEED_NEG*10, pax::ITER_SEEK_POS_BEGIN), "");


  // *SEEK ITER_SEEK_POS_CUR  cases *
  // case seek from middle
  iterator->Seek(SEEK_BEGIN_POS, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM/2, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(SEEK_OFFSET_POS, pax::ITER_SEEK_POS_CUR);
  ASSERT_EQ(iterator->getCurrentIndex(), MPARTITION_FILE_NUM/2 + SEEK_OFFSET_POS);
  iterator->Seek(SEEK_OFFSET_NEG, pax::ITER_SEEK_POS_CUR);
  ASSERT_EQ(iterator->getCurrentIndex(), MPARTITION_FILE_NUM/2);

  // case seek from middle excceds max bound
  iterator->Seek(SEEK_BEGIN_POS, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM/2, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(SEEK_OFFSET_EXCEED_POS, pax::ITER_SEEK_POS_CUR);
  ASSERT_EQ(iterator->getCurrentIndex(), MPARTITION_FILE_NUM-1);

  // case seek from middle excceds min bound
  iterator->Seek(SEEK_BEGIN_POS, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM/2, pax::ITER_SEEK_POS_BEGIN);
  iterator->Seek(SEEK_OFFSET_EXCEED_NEG, pax::ITER_SEEK_POS_CUR);
  ASSERT_EQ(iterator->getCurrentIndex(), SEEK_BEGIN_POS);


  // *SEEK ITER_SEEK_POS_END  cases *
  // case seek from end
  iterator->Seek(SEEK_OFFSET_POS, pax::ITER_SEEK_POS_END);
  ASSERT_EQ(iterator->getCurrentIndex(), MPARTITION_FILE_NUM - 1 - SEEK_OFFSET_POS);

  // case seek from middle excceds max bound
  ASSERT_DEATH(iterator->Seek(SEEK_OFFSET_EXCEED_POS*10, pax::ITER_SEEK_POS_END), "");

  // case seek from end excceds min bound
  ASSERT_DEATH(iterator->Seek(SEEK_OFFSET_EXCEED_NEG*10, pax::ITER_SEEK_POS_END), "");
}
}  // namespace pax::tests
