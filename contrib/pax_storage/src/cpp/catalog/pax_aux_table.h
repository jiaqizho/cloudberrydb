#pragma once

#include "comm/cbdb_api.h"

#include <memory>
#include <string>
#include <vector>

#include "catalog/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"

#define Anum_pg_pax_block_tables_ptblockname 1
#define Anum_pg_pax_block_tables_pttupcount 2
#define Anum_pg_pax_block_tables_ptblocksize 3
#define Natts_pg_pax_block_tables 3

namespace cbdb {
using MicroPartitionMetadataPtr = std::shared_ptr<pax::MicroPartitionMetadata>;

void GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                      NameData *compresstype,
                                      int *compresslevel);

void InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount,
                         int ptblocksize);

void PaxCreateMicroPartitionTable(const Relation rel,
                                  const RelFileNode *newrnode,
                                  char persistence);

void GetAllBlockFileInfo_PG_PaxBlock_Relation(
    std::shared_ptr<std::vector<MicroPartitionMetadataPtr>> result,
    const Relation relation, const Relation pg_blockfile_rel,
    const Snapshot paxMetaDataSnapshot);

void GetAllMicroPartitionMetadata(
    const Relation parentrel, const Snapshot paxMetaDataSnapshot,
    std::shared_ptr<std::vector<MicroPartitionMetadataPtr>> result);

}  // namespace cbdb
