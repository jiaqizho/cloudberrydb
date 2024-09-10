#pragma once

#include <stddef.h>
#include <stdint.h>

#include <map>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxEncoder {
 public:
  struct EncodingOption {
    ColumnEncoding_Kind column_encode_type;
    bool is_sign;
    int compress_level;

    ColumnEncoding_Kind lengths_encode_type;
    int lengths_compress_level;

    EncodingOption()
        : column_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED),
          is_sign(true),
          compress_level(0),
          lengths_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
          lengths_compress_level(0) {}
  };

 public:
  explicit PaxEncoder(const EncodingOption &encoder_options);

  void SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer);

  virtual ~PaxEncoder() = default;

  virtual void Append(char *data, size_t size) = 0;

  virtual bool SupportAppendNull() const = 0;

  virtual void Flush() = 0;

  virtual char *GetBuffer() const;

  virtual size_t GetBufferSize() const;

  /**
   * steaming encoder
   *
   * streaming means it need hold two DataBuffers
   * - one of DataBuffer used to temp save buffer
   * - one of DataBuffer used to keep result
   *
   * compared with the block method, streaming can reduce one memory copy
   */
  static std::shared_ptr<PaxEncoder> CreateStreamingEncoder(
      const EncodingOption &encoder_options, bool non_fixed = false);

 protected:
  const EncodingOption &encoder_options_;
  std::shared_ptr<DataBuffer<char>> result_buffer_;
};

}  // namespace pax
