
#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"

namespace pax {

/* This is a limitation of the orc protocol.
 * This parameters should not be changed under any circumstances.
 */
#define ORC_MAX_LITERAL_SIZE 512
#define ORC_MIN_REPEAT 3
#define ORC_HIST_LEN 32
#define ORC_MAX_SHORT_REPEAT_LENGTH (ORC_MIN_REPEAT + 7)

enum PaxColumnEncodeType {
  kTypeDefaultEncoded = 0,  // default encode by column type
  kTypeNoEncoded,           // no encoded
  kTypeRLEV2,               // used orc rle v2
  kTypeDirectDelta          // used delta
};

// FIXME(jiaqizho): change to ORCEncodingType
enum EncodingType {
  kShortRepeat = 0,
  kDirect,
  kPatchedBase,
  kDelta,
  // internal used, will not be serialized to disk
  kInvalidType,
};

struct FixedBitSizes {
  enum FBS {
    kONE = 0,
    kTWO,
    kTHREE,
    kFOUR,
    kFIVE,
    kSIX,
    kSEVEN,
    kEIGHT,
    kNINE,
    kTEN,
    kELEVEN,
    kTWELVE,
    kTHIRTEEN,
    kFOURTEEN,
    kFIFTEEN,
    kSIXTEEN,
    kSEVENTEEN,
    kEIGHTEEN,
    kNINETEEN,
    kTWENTY,
    kTWENTYONE,
    kTWENTYTWO,
    kTWENTYTHREE,
    kTWENTYFOUR,
    kTWENTYSIX,
    kTWENTYEIGHT,
    kTHIRTY,
    kTHIRTYTWO,
    kFORTY,
    kFORTYEIGHT,
    kFIFTYSIX,
    kSIXTYFOUR,
    kSIZE
  };
};

extern const uint8_t kFBSToBitWidthMap[FixedBitSizes::kSIZE];
extern const uint8_t kClosestBitsMap[65];
extern const uint8_t kClosestAlignedBitsMap[65];
extern const uint8_t kBitWidthToFBSMap[65];

inline uint32 DecodeBits(uint32 n) {  //
  return kFBSToBitWidthMap[n];
}

inline uint32 EncodeBits(uint32 n) {
  if (n <= 64) {
    return kBitWidthToFBSMap[n];
  } else {
    return FixedBitSizes::kSIXTYFOUR;
  }
}

uint32 GetClosestBits(uint32 n);
uint32 GetClosestAlignedBits(uint32 n);
uint32 FindClosestBits(int64 value);

// histogram functions
void BuildHistogram(int32 *histogram, int64_t *data, size_t number);
uint32_t GetPercentileBits(const int32 *histogram, size_t histogram_len,
                           double p);

// zig zag encoding for the sign number
inline int64 ZigZag(int64 value) {  //
  return (value << 1) ^ (value >> 63);
}

inline int64 UnZigZag(uint64 value) {  //
  return (value >> 1) ^ -(value & 1);
}

void ZigZagBuffers(int64_t *input, int64_t *output, size_t number);

}  // namespace pax
