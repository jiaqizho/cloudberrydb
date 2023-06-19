#include <gtest/gtest.h>

#include "catalog/table_metadata.h"
#include <utility>

extern int gp_debug_linger;

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
  void SetUp() override { gp_debug_linger = 0; }

  void TearDown() override {}
};

TEST_F(TableMetadataTest, new_iterator) {
  std::string block_id = "block_id";
  std::string file_name = "file_name";
  std::vector<pax::MicroPartitionMetadata> micro_partitions;

  pax::MicroPartitionMetadata meta_info(block_id, file_name);

  meta_info.SetTupleCount(1);
  meta_info.SetFileSize(MPARTITION_FILE_SIZE);

  micro_partitions.push_back(std::move(meta_info));

  std::unique_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(std::move(micro_partitions));

  ASSERT_FALSE(iterator->Empty());
  ASSERT_EQ(iterator->Size(), 1);
  ASSERT_FALSE(iterator->HasNext());

  pax::MicroPartitionMetadata meta_info_cur = iterator->Current();
  ASSERT_EQ(meta_info_cur.GetFileName(), file_name);
  ASSERT_EQ(meta_info_cur.GetTupleCount(), 1);
  ASSERT_EQ(meta_info_cur.GetFileSize(), MPARTITION_FILE_SIZE);
}

TEST_F(TableMetadataTest, seek_iterator) {
  std::string block_id = "block_id";
  std::string file_name = "file_name";
  std::vector<pax::MicroPartitionMetadata> micro_partitions;

  for (int i = 0; i < MPARTITION_FILE_NUM; i++) {
    pax::MicroPartitionMetadata meta_info(block_id, file_name);
    meta_info.SetTupleCount(1);
    meta_info.SetFileSize(MPARTITION_FILE_SIZE);
    micro_partitions.push_back(std::move(meta_info));
  }

  std::unique_ptr<pax::TableMetadata::Iterator> iterator =
      pax::TableMetadata::Iterator::Create(std::move(micro_partitions));

  ASSERT_TRUE(iterator->HasNext());

  // *SEEK BEGIN  cases *
  // case seek from begin
  iterator->Seek(SEEK_BEGIN_POS, pax::BEGIN);
  ASSERT_EQ(iterator->Index(), SEEK_BEGIN_POS);

  // case seek from begin exceed max bound.
  ASSERT_EQ(iterator->Seek(SEEK_OFFSET_EXCEED_POS * 10, pax::BEGIN),
            MPARTITION_FILE_NUM - 1);

  // case seek from begin exceed min bound.
  ASSERT_EQ(iterator->Seek(SEEK_OFFSET_EXCEED_NEG * 10, pax::BEGIN), 0);

  // *SEEK CURRENT  cases *
  // case seek from middle
  iterator->Seek(SEEK_BEGIN_POS, pax::BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM / 2, pax::BEGIN);
  iterator->Seek(SEEK_OFFSET_POS, pax::CURRENT);
  ASSERT_EQ(iterator->Index(), MPARTITION_FILE_NUM / 2 + SEEK_OFFSET_POS);
  iterator->Seek(SEEK_OFFSET_NEG, pax::CURRENT);
  ASSERT_EQ(iterator->Index(), MPARTITION_FILE_NUM / 2);

  // case seek from middle excceds max bound
  iterator->Seek(SEEK_BEGIN_POS, pax::BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM / 2, pax::BEGIN);
  iterator->Seek(SEEK_OFFSET_EXCEED_POS, pax::CURRENT);
  ASSERT_EQ(iterator->Index(), MPARTITION_FILE_NUM - 1);

  // case seek from middle excceds min bound
  iterator->Seek(SEEK_BEGIN_POS, pax::BEGIN);
  iterator->Seek(MPARTITION_FILE_NUM / 2, pax::BEGIN);
  iterator->Seek(SEEK_OFFSET_EXCEED_NEG, pax::CURRENT);
  ASSERT_EQ(iterator->Index(), SEEK_BEGIN_POS);

  // *SEEK END  cases *
  // case seek from end
  iterator->Seek(SEEK_OFFSET_POS, pax::END);
  ASSERT_EQ(iterator->Index(), MPARTITION_FILE_NUM - 1 - SEEK_OFFSET_POS);

  // case seek from middle excceds max bound
  ASSERT_EQ(iterator->Seek(SEEK_OFFSET_EXCEED_POS * 10, pax::END), 0);

  // case seek from end excceds min bound
  ASSERT_EQ(iterator->Seek(SEEK_OFFSET_EXCEED_NEG * 10, pax::END),
            MPARTITION_FILE_NUM - 1);
}
}  // namespace pax::tests
