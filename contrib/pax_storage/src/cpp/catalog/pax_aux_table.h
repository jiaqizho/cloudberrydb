#pragma once

#include "comm/cbdb_api.h"

#include <memory>
#include <string>
#include <vector>

#include "catalog/micro_partition_metadata.h"
#include "catalog/pax_aux_table.h"
#include "storage/local_file_system.h"

#define Anum_pg_pax_block_tables_ptblockname 1
#define Anum_pg_pax_block_tables_pttupcount 2
#define Anum_pg_pax_block_tables_ptblocksize 3
#define Natts_pg_pax_block_tables 3

namespace pax {
class CCPaxAuxTable final {
 public:
  CCPaxAuxTable() = default;
  ~CCPaxAuxTable() {}

  static void PaxAuxRelationSetNewFilenode(Relation rel,
                                           const RelFileNode *newrnode,
                                           char persistence);

  static void PaxAuxRelationNontransactionalTruncate(Relation rel);

  static void PaxAuxRelationCopyData(Relation rel,
                                     const RelFileNode *newrnode);

  static void PaxAuxRelationFileUnlink(RelFileNode node,
                                       BackendId backend,
                                       bool delete_topleveldir);
};
}  // namespace pax

namespace cbdb {

void GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                      NameData *compresstype,
                                      int *compresslevel);

void InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount,
                         int ptblocksize);

void AddMicroPartitionEntry(const pax::WriteSummary &summary);

void GetAllBlockFileInfo_PG_PaxBlock_Relation(
    std::vector<pax::MicroPartitionMetadata>
        &result,  // NOLINT(runtime/references)
    const Relation relation, const Relation pg_blockfile_rel,
    const Snapshot pax_meta_data_snapshot);

void GetAllMicroPartitionMetadata(const Relation parentrel,
                                  const Snapshot pax_meta_data_snapshot,
                                  std::vector<pax::MicroPartitionMetadata>
                                      &result);  // NOLINT(runtime/references)

void AddMicroPartitionEntry(const pax::WriteSummary &summary);

void DeleteMicroPartitionEntry(const Oid rel_oid,
                               const Snapshot pax_meta_data_snapshot,
                               const std::string block_id);
void PaxTransactionalTruncateTable(Oid aux_relid);

void PaxNontransactionalTruncateTable(Relation rel);

void PaxCreateMicroPartitionTable(const Relation rel);
}  // namespace cbdb

namespace paxc {
void CPaxTransactionalTruncateTable(Oid aux_relid);

void CPaxNontransactionalTruncateTable(Relation rel);

void CPaxCreateMicroPartitionTable(const Relation rel);
}  // namespace paxc
