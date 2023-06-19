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

#define PAX_TUPLE_ID_MAX_ROW_NUM INT64CONST((1 << (PAX_TUPLE_BIT_SIZE - 1)) - 1)

// | block number (24 bits) | tuple number (23 bits) |
// | (16 bits) | 8bit       |8bit |1bit |7bit | 8bit |
struct PaxItemPointer final {
  uint16 bytes_0_1;
  uint16 bytes_2_3;
  uint16 bytes_4_5;
  PaxItemPointer() {
    bytes_0_1 = 0;
    bytes_2_3 = 0;
    bytes_4_5 = 0;
  }
  PaxItemPointer(uint8 table_no, uint32 block_number, uint32 tuple_number) {
    bytes_0_1 = (table_no << BLOCK_NO_BITS_IN_BYTES_0_1);
    bytes_0_1 |= (block_number >> BLOCK_NO_BITS_IN_BYTES_2_3);

    // |7bit 9bit|11 bit 5 biy| 16bit|

    bytes_2_3 |= (block_number & BLOCK_NO_MASK_IN_BYTES_2_3)
                 << TUPLE_NO_BITS_IN_BYTES_2_3;
    bytes_2_3 = (tuple_number >> 15);

    bytes_4_5 = (tuple_number & 0x7FFF) + 1;
  }

  explicit PaxItemPointer(const PaxItemPointer *tid) {
    bytes_0_1 = tid->bytes_0_1;
    bytes_2_3 = tid->bytes_2_3;
    bytes_4_5 = tid->bytes_4_5;
  }

  inline bool Valid() const { return bytes_4_5 != 0; }
  static ItemPointerData GetTupleId(uint8 table_no, uint32 block_number,
                                    uint32 tuple_number) {
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

  uint8 GetTableNo() const { return bytes_0_1 >> BLOCK_NO_BITS_IN_BYTES_0_1; }

  uint32 GetBlockNumber() const {
    Assert(Valid());
    // get block_number in bytes_0_1
    uint32 block_number = (bytes_0_1 & BLOCK_NO_MASK_IN_BYTES_0_1)
                          << BLOCK_NO_BITS_IN_BYTES_2_3;
    block_number |= (bytes_2_3 >> TUPLE_NO_BITS_IN_BYTES_2_3);
    return block_number;
  }
  uint32 GetTupleNumber() const {
    Assert(Valid());
    return bytes_4_5 - 1 + ((bytes_2_3 & TUPLE_NO_MASK_IN_BYTES_2_3) << 15);
  }

  void Clear() {
    bytes_0_1 = 0;
    bytes_2_3 = 0;
    bytes_4_5 = 0;
  }
};
}  // namespace pax
