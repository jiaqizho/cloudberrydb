#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxDecoder {
 public:
  struct DecodingOption {
    PaxColumnEncodeType column_encode_type;
    bool is_sign;

    DecodingOption()
        : column_encode_type(PaxColumnEncodeType::kTypeNoEncoded),
          is_sign(false) {}
  };

  explicit PaxDecoder(const DecodingOption &decoder_options);

  virtual ~PaxDecoder() = default;

  virtual void SetDataBuffer(DataBuffer<char> *result_buffer);

  virtual size_t Next(const char *not_null) = 0;

  virtual size_t Decoding() = 0;

  virtual size_t Decoding(const char *not_null, size_t not_null_len) = 0;

  virtual char *GetBuffer() const;

  virtual size_t GetBufferSize() const;

  static PaxDecoder *CreateDecoder(const DecodingOption &decoder_options,
                                   char *raw_buffer, size_t buffer_len);

 protected:
  const DecodingOption &decoder_options_;
  DataBuffer<char> *result_buffer_;
};

}  // namespace pax
