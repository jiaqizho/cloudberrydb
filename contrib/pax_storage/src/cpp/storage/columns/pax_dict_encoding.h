#pragma once
#include "comm/pax_memory.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

struct PaxDictHead {
  uint64_t indexsz;
  uint64_t dictsz;
  uint64_t dict_descsz;
};

static_assert(sizeof(struct PaxDictHead) == 24,
              "PaxDictHead is not a align structure.");

class PaxDictEncoder final : public PaxEncoder {
 public:
  explicit PaxDictEncoder(const EncodingOption &encoder_options);

  ~PaxDictEncoder() override = default;

  void Append(char *data, size_t len) override;

  bool SupportAppendNull() const override;

  void Flush() override;

 private:
  size_t AppendInternal(char *data, size_t len);

  struct DictEntry {
    DictEntry(const char *buffer, size_t length) : data(buffer), len(length) {}
    const char *data;
    size_t len;
  };

  struct LessThan {
    bool operator()(const DictEntry &left, const DictEntry &right) const {
      int ret = memcmp(left.data, right.data, std::min(left.len, right.len));

      if (ret != 0) {
        return ret < 0;
      }

      return left.len < right.len;
    }
  };

 private:
  bool flushed_;

  std::vector<std::vector<char>> data_holder_;
  std::map<DictEntry, size_t, LessThan> dict_;
};

class PaxDictDecoder final : public PaxDecoder {
 public:
  explicit PaxDictDecoder(const PaxDecoder::DecodingOption &encoder_options);

  ~PaxDictDecoder() override;

  PaxDecoder *SetSrcBuffer(char *data, size_t data_len) override;

  PaxDecoder *SetDataBuffer(DataBuffer<char> *result_buffer) override;

  const char *GetBuffer() const override;

  size_t GetBufferSize() const override;

  size_t Next(const char *not_null) override;

  size_t Decoding() override;

  size_t Decoding(const char *not_null, size_t not_null_len) override;

 private:
  std::tuple<uint64, uint64, uint64> DecodeLens();

 private:
  DataBuffer<char> *data_buffer_;
  DataBuffer<char> *result_buffer_;
};

}  // namespace pax
