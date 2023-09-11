#include "storage/columns/pax_column.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_int.h"
#include "storage/columns/pax_encoding_column.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax::tests {

static void AppendInt4All(PaxColumn *pax_column, size_t bits) {
  int64 data;
  for (int16 i = INT16_MIN; i <= INT16_MAX; ++i) {  // dead loop
    data = i;
    pax_column->Append(reinterpret_cast<char *>(&data), bits / 8);
    if (i == INT16_MAX) {
      break;
    }
  }
}

template <typename T>
static void VerifyInt4All(char *verify_buff, size_t verify_len) {
  ASSERT_NE(verify_buff, nullptr);
  ASSERT_EQ(verify_len, (UINT16_MAX + 1) * sizeof(T));

  auto verify_int64_buff = reinterpret_cast<T *>(verify_buff);

  uint32 index = 0;
  for (int16 i = INT16_MIN; i <= INT16_MAX; ++i) {
    ASSERT_EQ(i, verify_int64_buff[index++]);
    if (i == INT16_MAX) {
      break;
    }
  }
}

static void VerifyInt4All(char *verify_buff, size_t verify_len, size_t bits) {
  switch (bits) {
    case 16:
      VerifyInt4All<int16>(verify_buff, verify_len);
      break;
    case 32:
      VerifyInt4All<int32>(verify_buff, verify_len);
      break;
    case 64:
      VerifyInt4All<int64>(verify_buff, verify_len);
      break;
    default:
      ASSERT_TRUE(false);
  }
}

static PaxColumn *CreateEncodeColumn(
    uint8 bits, const PaxEncoder::EncodingOption &encoding_option) {
  PaxColumn *int_column;

  switch (bits) {
    case 16:
      int_column = new PaxIntColumn<int16>(1024, std::move(encoding_option));
      break;
    case 32:
      int_column = new PaxIntColumn<int32>(1024, std::move(encoding_option));
      break;
    case 64:
      int_column = new PaxIntColumn<int64>(1024, std::move(encoding_option));
      break;
    default:
      int_column = nullptr;
      break;
  }
  return int_column;
}

static PaxColumn *CreateDecodeColumn(
    uint8 bits, size_t origin_lem,
    const PaxDecoder::DecodingOption &decoding_option, char *encoded_buff,
    size_t encoded_len) {
  switch (bits) {
    case 16: {
      auto *buffer_for_read = new DataBuffer<int16>(
          reinterpret_cast<int16 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      auto int_column = new PaxIntColumn<int16>(origin_lem / sizeof(int16),
                                                std::move(decoding_option));
      int_column->Set(buffer_for_read);

      return int_column;
    }
    case 32: {
      auto *buffer_for_read = new DataBuffer<int32>(
          reinterpret_cast<int32 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      auto int_column = new PaxIntColumn<int32>(origin_lem / sizeof(int32),
                                                std::move(decoding_option));
      int_column->Set(buffer_for_read);
      return int_column;
    }
    case 64: {
      auto *buffer_for_read = new DataBuffer<int64>(
          reinterpret_cast<int64 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      auto int_column = new PaxIntColumn<int64>(origin_lem / sizeof(int64),
                                                std::move(decoding_option));
      int_column->Set(buffer_for_read);
      return int_column;
    }
    default: {
      return nullptr;
    }
  }
  return nullptr;
}

class PaxColumnTest : public ::testing::Test {
 public:
  void SetUp() override {
    MemoryContext orc_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxColumn memory context", 80 * 1024 * 1024,
        80 * 1024 * 1024, 80 * 1024 * 1024);

    MemoryContextSwitchTo(orc_test_memory_context);
  }
};

class PaxColumnEncodingTest : public ::testing::TestWithParam<uint8> {
 public:
  void SetUp() override {
    MemoryContext orc_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxColumn memory context", 80 * 1024 * 1024,
        80 * 1024 * 1024, 80 * 1024 * 1024);

    MemoryContextSwitchTo(orc_test_memory_context);
  }
};

class PaxColumnCompressTest
    : public ::testing::TestWithParam<
          ::testing::tuple<uint8, ColumnEncoding_Kind>> {
 public:
  void SetUp() override {
    MemoryContext orc_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxColumn memory context", 800 * 1024 * 1024,
        800 * 1024 * 1024, 800 * 1024 * 1024);

    MemoryContextSwitchTo(orc_test_memory_context);
  }
};

class PaxNonFixedColumnCompressTest
    : public ::testing::TestWithParam<
          ::testing::tuple<uint8, ColumnEncoding_Kind, bool>> {
 public:
  void SetUp() override {
    MemoryContext orc_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxColumn memory context", 800 * 1024 * 1024,
        800 * 1024 * 1024, 800 * 1024 * 1024);

    MemoryContextSwitchTo(orc_test_memory_context);
  }
};

TEST_F(PaxColumnTest, FixColumnGetRangeBufferTest) {
  PaxColumn *column;
  char *buffer = nullptr;
  size_t buffer_len = 0;

  column = new PaxCommColumn<int32>(200);
  for (int32 i = 0; i < 16; i++) {
    column->Append(reinterpret_cast<char *>(&i), sizeof(int32));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int32));

  for (size_t i = 5; i < 16; i++) {
    auto *i_32 = reinterpret_cast<int32 *>(buffer + ((i - 5) * sizeof(int32)));
    ASSERT_EQ(*i_32, (int32)i);
  }
  ASSERT_EQ(column->GetRows(), 16);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16);

  column->Clear();

  for (int32 i = 0; i < 16; i++) {
    if (i % 3 == 0) {
      column->AppendNull();
    }
    column->Append(reinterpret_cast<char *>(&i), sizeof(int32));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int32));

  for (size_t i = 5; i < 16; i++) {
    auto *i_32 = reinterpret_cast<int32 *>(buffer + ((i - 5) * sizeof(int32)));
    ASSERT_EQ(*i_32, (int32)i);
  }

  ASSERT_EQ(column->GetRows(), 16 + 6);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16);

  delete column;
}

TEST_F(PaxColumnTest, NonFixColumnGetRangeBufferTest) {
  PaxColumn *column;
  char *buffer = nullptr;
  size_t buffer_len = 0;

  column = new PaxNonFixedColumn(200);
  for (int64 i = 0; i < 16; i++) {
    column->Append(reinterpret_cast<char *>(&i), sizeof(int64));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int64));

  for (size_t i = 5; i < 16; i++) {
    auto *i_32 = reinterpret_cast<int64 *>(buffer + ((i - 5) * sizeof(int64)));
    ASSERT_EQ(*i_32, (int64)i);
  }
  ASSERT_EQ(column->GetRows(), 16);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16);

  column->Clear();

  for (int64 i = 0; i < 16; i++) {
    if (i % 3 == 0) {
      column->AppendNull();
    }
    column->Append(reinterpret_cast<char *>(&i), sizeof(int64));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int64));

  for (size_t i = 5; i < 16; i++) {
    auto *i_32 = reinterpret_cast<int64 *>(buffer + ((i - 5) * sizeof(int64)));
    ASSERT_EQ(*i_32, (int64)i);
  }

  ASSERT_EQ(column->GetRows(), 16 + 6);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16);

  delete column;
}

TEST_P(PaxColumnEncodingTest, GetRangeEncodingColumnTest) {
  PaxColumn *int_column;
  auto bits = GetParam();
  if (bits < 32) {
    return;
  }

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  int64 data;
  for (int16 i = 0; i < 100; ++i) {
    data = i;
    int_column->Append(reinterpret_cast<char *>(&data), bits / 8);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, UINT16_MAX);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, (100) * bits / 8);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, origin_len, std::move(decoding_option), encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) =
      int_column_for_read->GetRangeBuffer(30, 20);

  for (int16 i = 0; i < 20; ++i) {
    switch (bits) {
      case 32:
        ASSERT_EQ((reinterpret_cast<int32 *>(verify_buff))[i], 30 + i);
        break;
      case 64:
        ASSERT_EQ((reinterpret_cast<int64 *>(verify_buff))[i], 30 + i);
        break;
    }
  }

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxColumnCompressTest, FixedCompressColumnGetRangeTest) {
  PaxColumn *int_column;
  auto bits = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_lvl = 5;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  int64 data;
  for (int16 i = 0; i < 100; ++i) {
    data = i;
    int_column->Append(reinterpret_cast<char *>(&data), bits / 8);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, UINT16_MAX);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, kind != ColumnEncoding_Kind_NO_ENCODED
                            ? (100) * bits / 8
                            : NO_ENCODE_ORIGIN_LEN);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;

  auto int_column_for_read =
      CreateDecodeColumn(bits, (100) * bits / 8, std::move(decoding_option),
                         encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) =
      int_column_for_read->GetRangeBuffer(30, 20);

  for (int16 i = 0; i < 20; ++i) {
    switch (bits) {
      case 32:
        ASSERT_EQ((reinterpret_cast<int32 *>(verify_buff))[i], 30 + i);
        break;
      case 64:
        ASSERT_EQ((reinterpret_cast<int64 *>(verify_buff))[i], 30 + i);
        break;
    }
  }

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnDefault) {
  PaxColumn *int_column;
  auto bits = GetParam();
  if (bits < 32) {
    return;
  }

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, UINT16_MAX);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, (UINT16_MAX + 1) * bits / 8);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, origin_len, std::move(decoding_option), encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnSpecType) {
  PaxColumn *int_column;
  auto bits = GetParam();

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, UINT16_MAX);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, (UINT16_MAX + 1) * bits / 8);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, origin_len, std::move(decoding_option), encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnNoEncoding) {
  PaxColumn *int_column;
  auto bits = GetParam();

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, NO_ENCODE_ORIGIN_LEN);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, encoded_len, std::move(decoding_option), encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxColumnCompressTest, PaxEncodingColumnCompressDecompress) {
  PaxColumn *int_column;
  auto bits = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_lvl = 5;
  encoding_option.is_sign = true;

  int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = int_column->GetOriginLength();
  ASSERT_EQ(origin_len, kind != ColumnEncoding_Kind_NO_ENCODED
                            ? (UINT16_MAX + 1) * bits / 8
                            : NO_ENCODE_ORIGIN_LEN);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;

  auto int_column_for_read =
      CreateDecodeColumn(bits, (UINT16_MAX + 1) * bits / 8,
                         std::move(decoding_option), encoded_buff, encoded_len);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);

  delete int_column;
  delete int_column_for_read;
}

TEST_P(PaxNonFixedColumnCompressTest,
       PaxEncodingNonFixedColumnCompressDecompress) {
  PaxNonFixedColumn *non_fixed_column;
  auto number = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());
  auto verify_range = ::testing::get<2>(GetParam());

  const size_t buffer_len = 1024;

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_lvl = 5;
  encoding_option.is_sign = true;

  non_fixed_column =
      new PaxNonFixedEncodingColumn(1024, std::move(encoding_option));

  std::srand(static_cast<unsigned int>(std::time(0)));
  char *data = reinterpret_cast<char *>(cbdb::Palloc(buffer_len * number));

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);

  for (size_t i = 0; i < buffer_len; ++i) {
    for (size_t j = 0; j < number; ++j) {
      data[j + i * number] = static_cast<char>(dis(gen));
    }
    non_fixed_column->Append((data + i * number), number);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = non_fixed_column->GetBuffer();
  auto length_buffer = non_fixed_column->GetLengthBuffer();
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = non_fixed_column->GetOriginLength();
  ASSERT_EQ(origin_len, kind != ColumnEncoding_Kind_NO_ENCODED
                            ? buffer_len * number
                            : NO_ENCODE_ORIGIN_LEN);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;

  auto non_fixed_column_for_read = new PaxNonFixedEncodingColumn(
      buffer_len * number, std::move(decoding_option));
  auto data_buffer_for_read =
      new DataBuffer<char>(encoded_buff, encoded_len, false, false);
  data_buffer_for_read->Brush(encoded_len);
  auto length_buffer_cpy = new DataBuffer<int64>(*length_buffer);
  non_fixed_column_for_read->Set(data_buffer_for_read, length_buffer_cpy,
                                 origin_len);

  char *verify_buff;
  size_t verify_len;

  if (verify_range) {
    std::tie(verify_buff, verify_len) =
        non_fixed_column_for_read->GetRangeBuffer(30, 50);
    ASSERT_EQ(verify_len, number * (50));

    for (size_t i = 0; i < verify_len; ++i) {
      EXPECT_EQ(verify_buff[i], data[i + (30 * number)]);
    }
  } else {
    std::tie(verify_buff, verify_len) = non_fixed_column_for_read->GetBuffer();
    ASSERT_EQ(verify_len, buffer_len * number);

    for (size_t i = 0; i < buffer_len * number; ++i) {
      EXPECT_EQ(verify_buff[i], data[i]);
    }
  }

  delete data;
  delete non_fixed_column;
  delete non_fixed_column_for_read;
}

INSTANTIATE_TEST_CASE_P(PaxColumnEncodingTestCombine, PaxColumnEncodingTest,
                        testing::Values(16, 32, 64));

INSTANTIATE_TEST_CASE_P(
    PaxColumnEncodingTestCombine, PaxColumnCompressTest,
    testing::Combine(testing::Values(16, 32, 64),
                     testing::Values(ColumnEncoding_Kind_NO_ENCODED,
                                     ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB)));

INSTANTIATE_TEST_CASE_P(
    PaxColumnEncodingTestCombine, PaxNonFixedColumnCompressTest,
    testing::Combine(testing::Values(16, 32, 64),
                     testing::Values(ColumnEncoding_Kind_NO_ENCODED,
                                     ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB),
                     testing::Values(true, false)));

};  // namespace pax::tests
