#include "storage/columns/pax_compress.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/columns/pax_encoding_utils.h"

namespace pax::tests {
class PaxCompressTest : public ::testing::TestWithParam<
                            ::testing::tuple<ColumnEncoding_Kind, uint32>> {
  void SetUp() override { CreateMemoryContext(); }
};

TEST_P(PaxCompressTest, TestCompressAndDecompress) {
  ColumnEncoding_Kind type = ::testing::get<0>(GetParam());
  uint32 data_len = ::testing::get<1>(GetParam());
  size_t dst_len = 0;
  PaxCompressor *compressor;

  char *data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  char *result_data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  compressor = PaxCompressor::CreateBlockCompressor(type);

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data =
      reinterpret_cast<char *>(cbdb::RePalloc(result_data, bound_size));
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  delete data;
  delete result_data;
}

INSTANTIATE_TEST_SUITE_P(
    PaxCompressTestCombined, PaxCompressTest,
    testing::Combine(testing::Values(ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB),
                     testing::Values(1, 128, 4096, 1024 * 1024,
                                     64 * 1024 * 1024)));

class PaxCompressTest2 : public ::testing::Test {
  void SetUp() override { CreateMemoryContext(); }
};

TEST_F(PaxCompressTest2, TestPgLZCompress) {
  size_t dst_len = 0;
  PaxCompressor *compressor = new PgLZCompressor();
  // too small may cause compress failed
  uint32 data_len = 512;

  char *data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  char *result_data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data =
      reinterpret_cast<char *>(cbdb::RePalloc(result_data, bound_size));
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  delete data;
  delete result_data;
}

#ifdef USE_LZ4
TEST_F(PaxCompressTest2, TestLZ4Compress) {
  size_t dst_len = 0;
  PaxCompressor *compressor = new PaxLZ4Compressor();
  uint32 data_len = 100;

  char *data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  char *result_data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data =
      reinterpret_cast<char *>(cbdb::RePalloc(result_data, bound_size));
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  delete data;
  delete result_data;
}
#endif

}  // namespace pax::tests
