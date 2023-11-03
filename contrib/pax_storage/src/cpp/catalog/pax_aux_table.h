#pragma once
#include "comm/cbdb_api.h"

#include <string>

#include "catalog/pax_aux_table.h"
#include "storage/micro_partition_metadata.h"

#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME 1
#define ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT 2
#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE 3
#define ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS 4
#define NATTS_PG_PAX_BLOCK_TABLES 4

namespace pax {
class CCPaxAuxTable final {
 public:
  CCPaxAuxTable() = delete;
  ~CCPaxAuxTable() = delete;

  static void PaxAuxRelationSetNewFilenode(Relation rel,
                                           const RelFileNode *newrnode,
                                           char persistence);

  static void PaxAuxRelationNontransactionalTruncate(Relation rel);

  static void PaxAuxRelationCopyData(Relation rel, const RelFileNode *newrnode,
                                     bool createnewpath = true);

  static void PaxAuxRelationCopyDataForCluster(Relation old_rel,
                                               Relation new_rel);

  static void PaxAuxRelationFileUnlink(RelFileNode node, BackendId backend,
                                       bool delete_topleveldir);
};
}  // namespace pax

namespace cbdb {

Oid GetPaxAuxRelid(Oid relid);

void AddMicroPartitionEntry(const pax::WriteSummary &summary);

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot,
                               const std::string &block_id);

}  // namespace cbdb
