#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxEncoder {
 public:
  struct EncodingOption {
    PaxColumnEncodeType column_encode_type;
    bool is_sign;

    EncodingOption()
        : column_encode_type(PaxColumnEncodeType::kTypeRLEV2), is_sign(false) {}
  };

 public:
  explicit PaxEncoder(const EncodingOption &encoder_options);

  void SetDataBuffer(DataBuffer<char> *result_buffer);

  virtual ~PaxEncoder() = default;

  virtual void Append(int64 data) = 0;

  virtual void Flush() = 0;

  virtual char *GetBuffer() const;

  virtual size_t GetBufferSize() const;

  static PaxEncoder *CreateEncoder(const EncodingOption &encoder_options);

 protected:
  const EncodingOption &encoder_options_;
  DataBuffer<char> *result_buffer_;
};

}  // namespace pax
