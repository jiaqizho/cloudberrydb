#include "storage/columns/pax_decoding.h"

#include "storage/columns/pax_rlev2_decoding.h"

namespace pax {

PaxDecoder *PaxDecoder::CreateDecoder(const DecodingOption &decoder_options,
                                      char *raw_buffer, size_t buffer_len) {
  PaxDecoder *decoder = nullptr;
  switch (decoder_options.column_encode_type) {
    case PaxColumnEncodeType::kTypeNoEncoded: {
      // do nothing
      break;
    }
    case PaxColumnEncodeType::kTypeRLEV2: {
      decoder = new PaxOrcDecoder(decoder_options, raw_buffer, buffer_len);
      break;
    }
    case PaxColumnEncodeType::kTypeDirectDelta: {
      /// TODO support it
      break;
    }
    case PaxColumnEncodeType::kTypeDefaultEncoded:
    default: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
  }

  return decoder;
}

PaxDecoder::PaxDecoder(const DecodingOption &decoder_options)
    : decoder_options_(decoder_options), result_buffer_(nullptr) {}

void PaxDecoder::SetDataBuffer(DataBuffer<char> *result_buffer) {
  Assert(!result_buffer_ && result_buffer);
  result_buffer_ = result_buffer;
}

char *PaxDecoder::GetBuffer() const { return result_buffer_->GetBuffer(); }

size_t PaxDecoder::GetBufferSize() const { return result_buffer_->Used(); }

}  // namespace pax