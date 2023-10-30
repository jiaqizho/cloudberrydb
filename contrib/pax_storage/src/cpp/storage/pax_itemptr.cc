#include "storage/pax_itemptr.h"

#ifdef ENABLE_LOCAL_INDEX
namespace pax {
std::string GenerateBlockID(Relation /* relation */) {
  // TODO: generate sequential number as file name
  return "1";
}
}  // namespace pax

#else

#include <uuid/uuid.h>

#include "comm/cbdb_wrappers.h"
#include "storage/paxc_block_map_manager.h"
namespace cbdb {
static paxc::PaxBlockId GetBlockId(Oid table_rel_oid, uint8 table_no,
                                   uint32 block_number) {
  CBDB_WRAP_START;
  { return paxc::get_block_id(table_rel_oid, table_no, block_number); }
  CBDB_WRAP_END;
}

static uint32 GetBlockNumber(Oid table_rel_oid, uint32 table_index,
                             paxc::PaxBlockId block_id) {
  CBDB_WRAP_START;
  { return paxc::get_block_number(table_rel_oid, table_index, block_id); }
  CBDB_WRAP_END;
}

static void GetTableIndexAndTableNumber(Oid table_rel_oid, uint8 *table_no,
                                        uint32 *table_index) {
  CBDB_WRAP_START;
  {
    paxc::get_table_index_and_table_number(table_rel_oid, table_no,
                                           table_index);
  }
  CBDB_WRAP_END;
}
}  // namespace cbdb

namespace pax {
std::string MapToBlockNumber(Relation rel, ItemPointerData ctid) {
  auto table_no = pax::GetTableNo(ctid);
  auto block_number = pax::GetBlockNumber(ctid);
  return cbdb::GetBlockId(rel->rd_id, table_no, block_number).ToStr();
}

uint32 BlockNumberManager::GetBlockNumber(Oid relid,
                                          const std::string &blockid) const {
  uint32 block_number = cbdb::GetBlockNumber(relid, table_index_,
                                             paxc::PaxBlockId(blockid.c_str()));
  return block_number;
}

void BlockNumberManager::InitOpen(Oid relid) {
  cbdb::GetTableIndexAndTableNumber(relid, &table_no_, &table_index_);
}

std::string GenerateBlockID(Relation /* relation */) {
  uuid_t uuid;
  char str[BLOCK_ID_SIZE + 1];

  uuid_generate(uuid);
  uuid_unparse(uuid, str);

  return std::string(str);
}
}  // namespace pax
#endif
