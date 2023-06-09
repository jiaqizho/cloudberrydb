#include <gtest/gtest.h>

#include "storage/pax_itemptr.h"

#include "comm/cbdb_api.h"

namespace pax::tests {
class PaxItemPtrTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(PaxItemPtrTest, get_block_number) {
  PaxItemPointer* tid = new PaxItemPointer();
  tid->bytes_0_1_ = 0xffff;
  tid->bytes_2_3_ = 0xff00;
  tid->bytes_4_5_ = 0;

  PaxItemPointer pax_tid_1(tid);
  EXPECT_EQ(pax_tid_1.Valid(), false);

  ItemPointerData htid;

  htid = PaxItemPointer::GetTupleId(0, 0xff, 1);
  PaxItemPointer pax_tid_2(reinterpret_cast<PaxItemPointer*>(&htid));
  EXPECT_EQ(pax_tid_2.GetTableNo(), 0);
  EXPECT_EQ(pax_tid_2.GetBlockNumber(), 0xff);
  EXPECT_EQ(pax_tid_2.GetTupleNumber(), 1);
  EXPECT_EQ(pax_tid_2.Valid(), true);

  htid = PaxItemPointer::GetTupleId(31, 0xffff, 0xff00);
  PaxItemPointer pax_tid_3(reinterpret_cast<PaxItemPointer*>(&htid));
  EXPECT_EQ(pax_tid_3.GetTableNo(), 31);
  EXPECT_EQ(pax_tid_3.GetBlockNumber(), 0xffff);
  EXPECT_EQ(pax_tid_3.GetTupleNumber(), 0xff00);
  EXPECT_EQ(pax_tid_3.Valid(), true);

  htid = PaxItemPointer::GetTupleId(0xf, 0xffff, PAXTupleId_MaxRowNum);
  PaxItemPointer pax_tid_4(reinterpret_cast<PaxItemPointer*>(&htid));
  EXPECT_EQ(pax_tid_4.GetTableNo(), 0xf);
  EXPECT_EQ(pax_tid_4.GetBlockNumber(), 0xffff);
  EXPECT_EQ(pax_tid_4.GetTupleNumber(), PAXTupleId_MaxRowNum);
  EXPECT_EQ(pax_tid_4.Valid(), true);

  htid = PaxItemPointer::GetTupleId(0x14, 0x12345, PAXTupleId_MaxRowNum);
  PaxItemPointer pax_tid_5(reinterpret_cast<PaxItemPointer*>(&htid));
  EXPECT_EQ(pax_tid_5.GetTableNo(), 0x14);
  EXPECT_EQ(pax_tid_5.GetBlockNumber(), 0x12345);
  EXPECT_EQ(pax_tid_5.GetTupleNumber(), PAXTupleId_MaxRowNum);
  EXPECT_EQ(pax_tid_5.Valid(), true);

  delete tid;
}
}  // namespace pax::tests
