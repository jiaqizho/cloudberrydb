#include "storage/columns/pax_compress.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"

namespace pax::tests {
class PaxCompressTest : public ::testing::TestWithParam<
                            ::testing::tuple<PaxColumnCompressType, uint32>> {
  void SetUp() override {
    MemoryContext pax_compress_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxCompressTestMemoryContext", 200 * 1024 * 1024,
        200 * 1024 * 1024, 200 * 1024 * 1024);

    MemoryContextSwitchTo(pax_compress_memory_context);
  }
};

TEST_P(PaxCompressTest, TestCompressAndDecompress) {
  PaxColumnCompressType type = ::testing::get<0>(GetParam());
  uint32 data_len = ::testing::get<1>(GetParam());
  uint32 dst_len = 0;
  PaxCompressor *compressor;
  PaxCompressor::CompressorOptions compress_options{};

  char *data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  char *result_data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  switch (type) {
    case PaxColumnCompressType::kTypeZSTD: {
      compressor = new PaxZSTDCompressor(compress_options);
      break;
    }
    case PaxColumnCompressType::kTypeZLIB: {
      compressor = new PaxZlibCompressor(compress_options);
      break;
    }
    default:
      break;
  }

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0);
  result_data = reinterpret_cast<char *>(cbdb::RePalloc(result_data, bound_size));
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_GT(dst_len, 0);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  delete data;
  delete result_data;
}

INSTANTIATE_TEST_CASE_P(PaxCompressTestCombined, PaxCompressTest,
                        testing::Combine(testing::Values(kTypeZSTD, kTypeZLIB),
                                         testing::Values(1, 128, 4096,
                                                         1024 * 1024,
                                                         64 * 1024 * 1024)));

}  // namespace pax::tests
