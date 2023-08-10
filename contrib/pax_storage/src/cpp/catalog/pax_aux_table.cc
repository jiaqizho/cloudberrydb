#include "catalog/pax_aux_table.h"

#include "comm/cbdb_api.h"

#include <uuid/uuid.h>

#include <utility>

#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_metadata.h"
#include "storage/paxc_block_map_manager.h"

namespace cbdb {
void GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                      NameData *compresstype,
                                      int *compresslevel) {
  CBDB_WRAP_START;
  {
    GetPaxTablesEntryAttributes(relid, blocksrelid, compresstype,
                                compresslevel);
  }
  CBDB_WRAP_END;
}

void InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount,
                         int ptblocksize) {
  Relation rel;
  HeapTuple tuple;
  NameData ptblockname;
  bool *nulls;
  Datum *values;
  int natts = 0;
  CBDB_WRAP_START;
  {
    rel = table_open(relid, RowExclusiveLock);
    natts = NATTS_PG_PAX_BLOCK_TABLES;
    values = reinterpret_cast<Datum *>(palloc(sizeof(Datum) * natts));
    nulls = reinterpret_cast<bool *>(palloc0(sizeof(bool) * natts));

    Assert(blockname);
    namestrcpy(&ptblockname, blockname);

    values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] =
        NameGetDatum(&ptblockname);
    nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;

    values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = Int32GetDatum(pttupcount);
    nulls[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = false;
    values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] =
        Int32GetDatum(ptblocksize);
    nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = false;
    tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    // insert a new tuple
    CatalogTupleInsert(rel, tuple);

    // Close the pax_tables_rel relcache entry without unlocking.
    // We have updated the catalog: consequently the lock must be
    // held until end of transaction.
    table_close(rel, NoLock);

    pfree(values);
    pfree(nulls);
  }
  CBDB_WRAP_END;
}

void GetAllBlockFileInfoPgPaxBlockRelation(
    std::vector<pax::MicroPartitionMetadata>
        &result,  // NOLINT(runtime/references)
    const Relation relation, Relation pg_blockfile_rel,
    const Snapshot pax_meta_data_snapshot) {
  TupleDesc pg_paxblock_dsc;
  HeapTuple tuple;
  SysScanDesc pax_scan;
  Datum blockid, tup_count;
  bool is_null;

  CBDB_WRAP_START;
  {
    pg_paxblock_dsc = RelationGetDescr(pg_blockfile_rel);
    pax_scan = systable_beginscan(pg_blockfile_rel, InvalidOid, false,
                                  pax_meta_data_snapshot, 0, NULL);

    while ((tuple = systable_getnext(pax_scan)) != NULL) {
      blockid = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                             pg_paxblock_dsc, &is_null);
      if (is_null)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("got invalid segno value: NULL")));

      std::string file_name =
          cbdb::BuildPaxFilePath(relation, DatumGetName(blockid)->data);

      pax::MicroPartitionMetadata meta_info(DatumGetName(blockid)->data,
                                            file_name);

      tup_count = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                               pg_paxblock_dsc, &is_null);
      if (is_null)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("got invalid tupcount value: NULL")));
      meta_info.SetTupleCount(tup_count);

      // TODO(gongxun): add more statistics info
      result.push_back(std::move(meta_info));
    }

    systable_endscan(pax_scan);
  }
  CBDB_WRAP_END;
}

void GetAllMicroPartitionMetadata(const Relation parentrel,
                                  const Snapshot pax_meta_data_snapshot,
                                  std::vector<pax::MicroPartitionMetadata>
                                      &result) {  // NOLINT(runtime/references)
  Relation pg_paxblock_rel;
  Oid block_rel_id;

  CBDB_WRAP_START;
  {
    GetPaxTablesEntryAttributes(parentrel->rd_id, &block_rel_id, NULL, NULL);

    if (block_rel_id == InvalidOid)
      elog(ERROR,
           "could not find pg_paxblock_rel aux table for pax table \"%s\"",
           RelationGetRelationName(parentrel));

    // Assert(RelationIsPax(parentrel));

    pg_paxblock_rel = table_open(block_rel_id, AccessShareLock);

    GetAllBlockFileInfoPgPaxBlockRelation(result, parentrel, pg_paxblock_rel,
                                             pax_meta_data_snapshot);

    table_close(pg_paxblock_rel, AccessShareLock);
  }
  CBDB_WRAP_END;
}

void DeletePaxBlockEntry(const Oid relid, const Snapshot pax_meta_data_snapshot,
                         const char *blockname) {
  Relation rel;
  ScanKeyData key[1];
  SysScanDesc scan;
  HeapTuple tuple;
  NameData ptblockname;

  CBDB_WRAP_START;
  {
    rel = table_open(relid, RowExclusiveLock);
    namestrcpy(&ptblockname, blockname);
    ScanKeyInit(&key[0], ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&ptblockname));

    // should add snapshot support
    scan = systable_beginscan(rel, InvalidOid, false, pax_meta_data_snapshot, 1,
                              key);

    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
      CatalogTupleDelete(rel, &tuple->t_self);
    }

    systable_endscan(scan);
    table_close(rel, RowExclusiveLock);
  }
  CBDB_WRAP_END;
}

void DeleteMicroPartitionEntry(const Oid rel_oid,
                               const Snapshot pax_meta_data_snapshot,
                               const std::string block_id) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(rel_oid, &pax_block_tables_rel_id,
                                         NULL, NULL);
  cbdb::DeletePaxBlockEntry(pax_block_tables_rel_id, pax_meta_data_snapshot,
                            block_id.c_str());
}

void AddMicroPartitionEntry(const pax::WriteSummary &summary) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(summary.rel_oid,
                                         &pax_block_tables_rel_id, NULL, NULL);
  cbdb::InsertPaxBlockEntry(pax_block_tables_rel_id, summary.block_id.c_str(),
                            summary.num_tuples, summary.file_size);
}

void PaxTransactionalTruncateTable(Oid aux_relid) {
  CBDB_WRAP_START;
  { paxc::CPaxTransactionalTruncateTable(aux_relid); }
  CBDB_WRAP_END;
}

void PaxNontransactionalTruncateTable(Relation rel) {
  CBDB_WRAP_START;
  { paxc::CPaxNontransactionalTruncateTable(rel); }
  CBDB_WRAP_END;
}

void PaxCreateMicroPartitionTable(const Relation rel) {
  CBDB_WRAP_START;
  { paxc::CPaxCreateMicroPartitionTable(rel); }
  CBDB_WRAP_END;
}
}  // namespace cbdb

namespace pax {
void CCPaxAuxTable::PaxAuxRelationSetNewFilenode(Relation rel,
                                                 const RelFileNode *newrnode,
                                                 char persistence) {
  HeapTuple tupcache;
  std::string path;
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  tupcache = cbdb::SearchSysCache(rel, PAXTABLESID);
  if (cbdb::TupleIsValid(tupcache)) {
    Oid aux_relid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;
    cbdb::PaxTransactionalTruncateTable(aux_relid);
    cbdb::ReleaseTupleCache(tupcache);
  } else {
    // create pg_pax_blocks_<pax_table_oid>
    cbdb::PaxCreateMicroPartitionTable(rel);
  }

  // Create pax table relfilenode file and database directory under path base/,
  // The relfilenode created here is to be compatible with PG normal process
  // logic instead of being used by pax storage.
  cbdb::RelationCreateStorageDirectory(*newrnode, persistence, SMGR_MD, rel);
  path = cbdb::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
  Assert(!path.empty());
  fs->CreateDirectory(path);
}

void CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(Relation rel) {
  cbdb::PaxNontransactionalTruncateTable(rel);

  // Delete all micro partition file on non-transactional truncate  but reserve
  // top level PAX file directory.
  PaxAuxRelationFileUnlink(rel->rd_node, rel->rd_backend, false);
}

void CCPaxAuxTable::PaxAuxRelationCopyData(Relation rel,
                                           const RelFileNode *newrnode) {
  std::string src_path;
  std::string dst_path;
  std::vector<std::string> filelist;

  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  src_path = cbdb::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
  Assert(!src_path.empty());

  // get micropatition file source folder filename list for copying.
  filelist = fs->ListDirectory(src_path);
  if (filelist.empty()) return;

  // create pg_pax_table relfilenode file and dbid directory under path
  // pg_tblspc/.
  cbdb::RelationCreateStorageDirectory(*newrnode, rel->rd_rel->relpersistence,
                                       SMGR_MD, rel);

  dst_path = cbdb::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
  Assert(!dst_path.empty());

  if (src_path.empty() || dst_path.empty())
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError);

  // create micropartition file destination folder for copying.
  if (cbdb::PathNameCreateDir(dst_path.c_str()) != 0)
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError);

  for (auto &iter : filelist) {
    Assert(!iter.empty());
    src_path.append("/");
    src_path.append(iter);
    dst_path.append("/");
    dst_path.append(iter);
    fs->CopyFile(src_path, dst_path);
  }

  // TODO(Tony) : here need to implement pending delete srcPath after set new
  // tablespace.
}

void CCPaxAuxTable::PaxAuxRelationFileUnlink(RelFileNode node,
                                             BackendId backend,
                                             bool delete_topleveldir) {
  std::string relpath;
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  // Delete all micro partition file directory.
  relpath = cbdb::BuildPaxDirectoryPath(node, backend);
  fs->DeleteDirectory(relpath, delete_topleveldir);
}
}  // namespace pax

namespace paxc {
void CPaxTransactionalTruncateTable(Oid aux_relid) {
  Relation aux_rel;
  Assert(OidIsValid(aux_relid));

  // truncate already exist pax block auxiliary table.
  aux_rel = relation_open(aux_relid, AccessExclusiveLock);

  /*TODO1 pending-delete operation should be applied here. */
  RelationSetNewRelfilenode(aux_rel, aux_rel->rd_rel->relpersistence);
  relation_close(aux_rel, NoLock);
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
void CPaxNontransactionalTruncateTable(Relation rel) {
  HeapTuple tupcache;
  Relation aux_rel;
  Oid aux_relid;

  tupcache = SearchSysCache1(PAXTABLESID, RelationGetRelid(rel));
  if (!HeapTupleIsValid(tupcache))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cache lookup failed with relid=%u for aux relation "
                           "in pg_pax_tables.",
                           RelationGetRelid(rel))));
  aux_relid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;

  Assert(OidIsValid(aux_relid));
  aux_rel = relation_open(aux_relid, AccessExclusiveLock);
  heap_truncate_one_rel(aux_rel);
  relation_close(aux_rel, NoLock);
  ReleaseSysCache(tupcache);
}

void CPaxCreateMicroPartitionTable(const Relation rel) {
  Relation pg_class_desc;
  char aux_relname[32];
  Oid relid;
  Oid aux_relid;
  Oid aux_namespace_id;
  Oid pax_relid;
  TupleDesc tupdesc;

  pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);
  pax_relid = RelationGetRelid(rel);

  // 1. create blocks table.
  snprintf(aux_relname, sizeof(aux_relname), "pg_pax_blocks_%u", pax_relid);
  aux_namespace_id = PG_PAXAUX_NAMESPACE;
  aux_relid = GetNewOidForRelation(pg_class_desc, ClassOidIndexId,
                                   Anum_pg_class_oid,  // new line
                                   aux_relname, aux_namespace_id);
  tupdesc = CreateTemplateTupleDesc(NATTS_PG_PAX_BLOCK_TABLES);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                     "ptblockname", NAMEOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                     "pttupcount", INT4OID, -1, 0);
  // TODO(chenhongjie): uncompressed and compressed ptblocksize are needed.
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE,
                     "ptblocksize", INT4OID, -1, 0);
  relid = heap_create_with_catalog(
      aux_relname, aux_namespace_id, InvalidOid, aux_relid, InvalidOid,
      InvalidOid, rel->rd_rel->relowner, HEAP_TABLE_AM_OID, tupdesc, NIL,
      RELKIND_RELATION, rel->rd_rel->relpersistence, rel->rd_rel->relisshared,
      RelationIsMapped(rel), ONCOMMIT_NOOP, NULL, /* GP Policy */
      (Datum)0, false,                            /* use _user_acl */
      true, true, InvalidOid, NULL,               /* typeaddress */
      false /* valid_opts */);
  Assert(relid == aux_relid);
  table_close(pg_class_desc, NoLock);

  // 2. insert entry into pg_pax_tables.
  InsertPaxTablesEntry(pax_relid, aux_relid, "", 0);

  // 3. record pg_depend, pg_pax_blocks_<xxx> depends relation.
  {
    ObjectAddress base;
    ObjectAddress aux;
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = RelationRelationId;
    aux.objectId = aux_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);

    // pg_pax_tables single row depend
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = PaxTablesRelationId;
    aux.objectId = pax_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
  }
}
}  // namespace paxc
