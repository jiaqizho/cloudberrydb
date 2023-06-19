#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax_block_id.h"
#include "storage/pax_itemptr.h"

namespace paxc {

#define BLOCK_MAPPING_ARRAY_SIZE 64
struct SharedTableBlockMappingData {
  Oid relid_;
  uint32 shared_size_block_ids_;
  uint32 shared_used_block_ids_;
  dsm_handle shared_block_ids_handle_;
  SharedTableBlockMappingData() {
    relid_ = InvalidOid;
    shared_size_block_ids_ = 0;
    shared_used_block_ids_ = 0;
    shared_block_ids_handle_ = 0;
  }
};

struct PaxXactSharedState {
  LWLock lock_;
  uint32 block_mapping_used_size_;
  SharedTableBlockMappingData shared_block_mapping_[BLOCK_MAPPING_ARRAY_SIZE];
};

struct LocalTableBlockMappingData {
  Oid relid_;
  uint32 size_block_ids_;
  uint32 used_block_ids_;
  dsm_segment *block_ids_segment_;
  PaxBlockId *block_ids_;
  LocalTableBlockMappingData() {
    relid_ = InvalidOid;
    size_block_ids_ = 0;
    used_block_ids_ = 0;
    block_ids_segment_ = nullptr;
    block_ids_ = nullptr;
  }
};

struct TableEntry {
  uint16 table_no;
  Oid relid_;
  uint32 table_index_;
};

struct XactHashKey {
  int session_id_;
  int command_id_;
};

struct XactHashEntry {
  XactHashKey key_;
  PaxXactSharedState shared_state_;
};

struct XactLockSlot {
  bool used;
};
// use this struct find which the lock slot is not used, and assigned it to hash
// table entry when sql command start
struct PaxSharedState {
  int pax_xact_lock_tranche_id_;
};

void pax_shmem_request();
void pax_shmem_startup();

void init_command_resource();
void release_command_resource();

void get_table_index_and_table_number(const Oid table_rel_oid, uint8 *table_no,
                                      uint32 *table_index);

uint32 get_block_number(const Oid table_rel_oid, const uint32 table_index,
                        const PaxBlockId block_id);
PaxBlockId get_block_id(const Oid table_rel_oid, const uint8 table_no,
                        const uint32 block_number);
}  // namespace paxc
