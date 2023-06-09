#pragma once

#include "comm/cbdb_api.h"

#include <assert.h>

#define BLOCK_ID_SIZE 36
namespace paxc {
struct PaxBlockId {
  char block_id_[BLOCK_ID_SIZE + 1];
  explicit PaxBlockId(const char* block_id) {
    Assert(strlen(block_id) == BLOCK_ID_SIZE);
    strncpy(block_id_, block_id, BLOCK_ID_SIZE);
    block_id_[BLOCK_ID_SIZE] = '\0';
  }

  const char* to_str() const { return block_id_; }
};
}  // namespace paxc
