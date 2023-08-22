#pragma once

#include "comm/cbdb_api.h"

#include <memory>
#include <string>
#include <vector>

#include "storage/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"
#include "storage/local_file_system.h"

#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME 1
#define ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT 2
#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE 3
#define ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS 4
#define NATTS_PG_PAX_BLOCK_TABLES 4

namespace pax {
class CCPaxAuxTable final {
 public:
  CCPaxAuxTable() = default;
  ~CCPaxAuxTable() = default;

  static void PaxAuxRelationSetNewFilenode(Relation rel,
                                           const RelFileNode *newrnode,
                                           char persistence);

  static void PaxAuxRelationNontransactionalTruncate(Relation rel);

  static void PaxAuxRelationCopyData(Relation rel, const RelFileNode *newrnode);

  static void PaxAuxRelationFileUnlink(RelFileNode node, BackendId backend,
                                       bool delete_topleveldir);
};
}  // namespace pax

namespace cbdb {

void GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                      NameData *compresstype,
                                      int *compresslevel);

void InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount,
                         int ptblocksize, const ::pax::stats::MicroPartitionStatisticsInfo &mp_stats);

void AddMicroPartitionEntry(const pax::WriteSummary &summary);

void GetAllBlockFileInfoPgPaxBlockRelation(
    std::vector<pax::MicroPartitionMetadata>
        &result,  // NOLINT(runtime/references)
    Relation relation, Relation pg_blockfile_rel,
    Snapshot pax_meta_data_snapshot);

void GetAllMicroPartitionMetadata(Relation parentrel,
                                  Snapshot pax_meta_data_snapshot,
                                  std::vector<pax::MicroPartitionMetadata>
                                      &result);  // NOLINT(runtime/references)

void AddMicroPartitionEntry(const pax::WriteSummary &summary);

void DeleteMicroPartitionEntry(Oid rel_oid, Snapshot pax_meta_data_snapshot,
                               std::string block_id);
void PaxTransactionalTruncateTable(Oid aux_relid);

void PaxNontransactionalTruncateTable(Relation rel);

void PaxCreateMicroPartitionTable(Relation rel);
}  // namespace cbdb

namespace paxc {
void CPaxTransactionalTruncateTable(Oid aux_relid);

void CPaxNontransactionalTruncateTable(Relation rel);

void CPaxCreateMicroPartitionTable(Relation rel);
}  // namespace paxc
