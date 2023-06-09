#include <gtest/gtest.h>

#include "comm/bitmap.h"
namespace pax::tests {
class BitMapTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(BitMapTest, test) {
  FixedBitmap bit_map(100);
  ASSERT_EQ(bit_map.Test(0), false);
  ASSERT_EQ(bit_map.Test(99), false);
  bit_map.Set(0);
  ASSERT_EQ(bit_map.Test(0), true);
  ASSERT_EQ(bit_map.Test(99), false);
  bit_map.Set(99);
  ASSERT_EQ(bit_map.Test(0), true);
  ASSERT_EQ(bit_map.Test(99), true);
  bit_map.Clear(0);
  ASSERT_EQ(bit_map.Test(0), false);
  ASSERT_EQ(bit_map.Test(99), true);
  bit_map.Clear(99);
  ASSERT_EQ(bit_map.Test(0), false);
  ASSERT_EQ(bit_map.Test(99), false);

  ASSERT_EQ(bit_map.Size(), 13);
}

TEST_F(BitMapTest, FixedBitmap) {
  FixedBitmap bit_map(100);
  bit_map.Set(0);
  bit_map.Set(50);
  bit_map.Set(99);

  BitmapIterator it(&bit_map);

  ASSERT_EQ(it.Next(true), 0);
  ASSERT_EQ(it.Next(true), 50);
  ASSERT_EQ(it.Next(true), 99);

  it.SeekTo(0);
  ASSERT_EQ(it.Next(false), 1);
  ASSERT_EQ(it.Next(false), 2);
  ASSERT_EQ(it.Next(false), 3);
}

TEST_F(BitMapTest, DynamicBitmap) {
  DynamicBitmap bit_map(100);
  bit_map.Set(0);
  bit_map.Set(50);
  bit_map.Set(99);

  BitmapIterator it(&bit_map);

  ASSERT_EQ(it.Next(true), 0);
  ASSERT_EQ(it.Next(true), 50);
  ASSERT_EQ(it.Next(true), 99);

  bit_map.Resize(200);
  bit_map.Set(100);
  bit_map.Set(150);
  bit_map.Set(199);

  ASSERT_EQ(it.Next(true), 100);
  ASSERT_EQ(it.Next(true), 150);
  ASSERT_EQ(it.Next(true), 199);

  it.SeekTo(0);
  ASSERT_EQ(it.Next(false), 1);
  ASSERT_EQ(it.Next(false), 2);
  ASSERT_EQ(it.Next(false), 3);
}
}  // namespace pax::tests
