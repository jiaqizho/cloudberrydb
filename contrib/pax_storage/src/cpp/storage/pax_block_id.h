#pragma once

#include "comm/cbdb_api.h"

#include <assert.h>

#define BLOCK_ID_SIZE 36
namespace paxc {
struct PaxBlockId {
  char pax_block_id[BLOCK_ID_SIZE + 1];
  explicit PaxBlockId(const char *block_id) {
    Assert(strlen(block_id) == BLOCK_ID_SIZE);
    strncpy(pax_block_id, block_id, BLOCK_ID_SIZE);
    pax_block_id[BLOCK_ID_SIZE] = '\0';
  }

  const char *ToStr() const { return pax_block_id; }
};
}  // namespace paxc
