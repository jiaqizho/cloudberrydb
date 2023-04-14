#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/pax_aux_table.h"
#include "catalog/micro_partition_metadata.h"
extern "C" {
#include "postgres.h"  // NOLINT
#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "cdb/cdbcustomam.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h" // NOLINT
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "uuid/uuid.h"
}

#define Anum_pg_pax_block_tables_ptblockname 1
#define Anum_pg_pax_block_tables_pttupcount 2
#define Natts_pg_pax_block_tables 2

namespace cbdb {
const std::string GenRandomBlockId();

void GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                      NameData *compresstype,
                                      int *compresslevel);

void InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount);

void PaxCreateMicroPartitionTable(Relation rel, Oid relfilenode);

void GetAllBlockFileInfo_PG_PaxBlock_Relation(
    std::shared_ptr<std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>> result,
    const Relation relation, const Relation pg_blockfile_rel,
    const Snapshot paxMetaDataSnapshot);

void GetAllMicroPartitionMetadata(
    const Relation parentrel, const Snapshot paxMetaDataSnapshot,
    std::shared_ptr<std::vector<std::shared_ptr<pax::MicroPartitionMetadata>>> result);

}  // namespace cbdb
