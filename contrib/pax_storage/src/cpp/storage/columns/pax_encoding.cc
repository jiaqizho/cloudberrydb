#include "storage/columns/pax_encoding.h"

#include "storage/columns/pax_rlev2_encoding.h"

#include <utility>

namespace pax {

PaxEncoder *PaxEncoder::CreateEncoder(const EncodingOption &encoder_options) {
  PaxEncoder *encoder = nullptr;
  switch (encoder_options.column_encode_type) {
    case PaxColumnEncodeType::kTypeNoEncoded: {
      // do nothing
      break;
    }
    case PaxColumnEncodeType::kTypeRLEV2: {
      encoder = new PaxOrcEncoder(std::move(encoder_options));
      break;
    }
    case PaxColumnEncodeType::kTypeDirectDelta: {
      break;
    }
    case PaxColumnEncodeType::kTypeDefaultEncoded:
    default: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
  }

  return encoder;
}

PaxEncoder::PaxEncoder(const EncodingOption &encoder_options)
    : encoder_options_(encoder_options), result_buffer_(nullptr) {}

void PaxEncoder::SetDataBuffer(DataBuffer<char> *result_buffer) {
  Assert(!result_buffer_ && result_buffer);
  Assert(result_buffer->IsMemTakeOver());
  result_buffer_ = result_buffer;
}

char *PaxEncoder::GetBuffer() const { return result_buffer_->GetBuffer(); }

size_t PaxEncoder::GetBufferSize() const { return result_buffer_->Used(); }

}  // namespace pax

