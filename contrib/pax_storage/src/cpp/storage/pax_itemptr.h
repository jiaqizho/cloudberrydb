#pragma once
#include "comm/cbdb_api.h"

#include <stdint.h>

#include <string>

namespace pax {
#define PAX_TABLE_NUM_BIT_SIZE 5
#define PAX_BLOCK_BIT_SIZE 22
#define PAX_TUPLE_BIT_SIZE (48 - 7 - PAX_BLOCK_BIT_SIZE)
#define MAX_TABLE_NUM_IN_CTID ((1 << PAX_TABLE_NUM_BIT_SIZE) - 1)

#define BLOCK_NO_BITS_IN_BYTES_0_1 (16 - PAX_TABLE_NUM_BIT_SIZE)
#define TUPLE_NO_BITS_IN_BYTES_2_3 (PAX_TUPLE_BIT_SIZE - 16)
#define BLOCK_NO_BITS_IN_BYTES_2_3 (16 - TUPLE_NO_BITS_IN_BYTES_2_3)

// 9bit
// 0x1ff
#define BLOCK_NO_MASK_IN_BYTES_0_1 (0xffff >> PAX_TABLE_NUM_BIT_SIZE)
// 11bit
// 0x07fff
#define BLOCK_NO_MASK_IN_BYTES_2_3 (0xffff >> TUPLE_NO_BITS_IN_BYTES_2_3)

// 5bit
// 0x001f
#define TUPLE_NO_MASK_IN_BYTES_2_3 (0xffff >> BLOCK_NO_BITS_IN_BYTES_2_3)

// #define PAX_BLOCK_BIT_IN_BI_LO_BITS (PAX_BLOCK_BIT_SIZE - 16)
// #define PAX_TUPLE_BIT_IN_BI_LO_BITS (32 - PAX_BLOCK_BIT_SIZE)
// #define PAX_TUPLE_BIT_IN_BI_LO_MASK (0xFFFF >> PAX_BLOCK_BIT_IN_BI_LO_BITS)

#define PAXTupleId_MaxRowNum INT64CONST((1 << (PAX_TUPLE_BIT_SIZE - 1)) - 1)

// | block number (24 bits) | tuple number (23 bits) |
// | (16 bits) | 8bit       |8bit |1bit |7bit | 8bit |
struct PaxItemPointer final {
  uint16 bytes_0_1_;
  uint16 bytes_2_3_;
  uint16 bytes_4_5_;
  PaxItemPointer() {
    bytes_0_1_ = 0;
    bytes_2_3_ = 0;
    bytes_4_5_ = 0;
  }
  PaxItemPointer(uint8_t table_no, uint32_t block_number,
                 uint32_t tuple_number) {
    bytes_0_1_ = (table_no << BLOCK_NO_BITS_IN_BYTES_0_1);
    bytes_0_1_ |= (block_number >> BLOCK_NO_BITS_IN_BYTES_2_3);

    // |7bit 9bit|11 bit 5 biy| 16bit|

    bytes_2_3_ |= (block_number & BLOCK_NO_MASK_IN_BYTES_2_3)
                  << TUPLE_NO_BITS_IN_BYTES_2_3;
    bytes_2_3_ = (tuple_number >> 15);

    bytes_4_5_ = (tuple_number & 0x7FFF) + 1;
  }

  explicit PaxItemPointer(const PaxItemPointer* tid) {
    bytes_0_1_ = tid->bytes_0_1_;
    bytes_2_3_ = tid->bytes_2_3_;
    bytes_4_5_ = tid->bytes_4_5_;
  }

  inline bool Valid() const { return bytes_4_5_ != 0; }
  static ItemPointerData GetTupleId(uint8_t table_no, uint32_t block_number,
                                    uint32_t tuple_number) {
    ItemPointerData tid;
    // table_no in bi_hi
    tid.ip_blkid.bi_hi = (table_no << BLOCK_NO_BITS_IN_BYTES_0_1);

    // block_number in bi_hi
    tid.ip_blkid.bi_hi |= (block_number >> BLOCK_NO_BITS_IN_BYTES_2_3);

    // |7bit 9bit|11 bit 5 biy| 16bit|

    // block_number in bi_lo
    tid.ip_blkid.bi_lo = (block_number & BLOCK_NO_MASK_IN_BYTES_2_3)
                         << TUPLE_NO_BITS_IN_BYTES_2_3;
    // tuple_number in bi_lo
    tid.ip_blkid.bi_lo |= (tuple_number >> 15);
    // tuple_number in ip_posid
    tid.ip_posid = (tuple_number & 0x7FFF) + 1;
    return tid;
  }

  uint8_t GetTableNo() const {
    return bytes_0_1_ >> BLOCK_NO_BITS_IN_BYTES_0_1;
  }

  uint32_t GetBlockNumber() const {
    Assert(Valid());
    // get block_number in bytes_0_1_
    uint32 block_number = (bytes_0_1_ & BLOCK_NO_MASK_IN_BYTES_0_1)
                          << BLOCK_NO_BITS_IN_BYTES_2_3;
    block_number |= (bytes_2_3_ >> TUPLE_NO_BITS_IN_BYTES_2_3);
    return block_number;
  }
  uint32_t GetTupleNumber() const {
    Assert(Valid());
    return bytes_4_5_ - 1 + ((bytes_2_3_ & TUPLE_NO_MASK_IN_BYTES_2_3) << 15);
  }

  void Clear() {
    bytes_0_1_ = 0;
    bytes_2_3_ = 0;
    bytes_4_5_ = 0;
  }
};
}  // namespace pax
