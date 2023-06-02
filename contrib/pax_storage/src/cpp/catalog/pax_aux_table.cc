#include "catalog/pax_aux_table.h"

#include "comm/cbdb_api.h"

#include <uuid/uuid.h>

#include "catalog/micro_partition_metadata.h"
#include "catalog/table_metadata.h"
#include "comm/cbdb_wrappers.h"
#include "comm/paxc_utils.h"

void cbdb::GetMicroPartitionEntryAttributes(Oid relid, Oid *blocksrelid,
                                            NameData *compresstype,
                                            int *compresslevel) {
  CBDB_WRAP_START;
  {
    GetPaxTablesEntryAttributes(relid, blocksrelid, compresstype,
                                compresslevel);
  }
  CBDB_WRAP_END;
}

void cbdb::InsertPaxBlockEntry(Oid relid, const char *blockname, int pttupcount,
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
    natts = Natts_pg_pax_block_tables;
    values = reinterpret_cast<Datum *>(palloc(sizeof(Datum) * natts));
    nulls = reinterpret_cast<bool *>(palloc0(sizeof(bool) * natts));

    Assert(blockname);
    namestrcpy(&ptblockname, blockname);

    values[Anum_pg_pax_block_tables_ptblockname - 1] =
        NameGetDatum(&ptblockname);
    nulls[Anum_pg_pax_block_tables_ptblockname - 1] = false;

    values[Anum_pg_pax_block_tables_pttupcount - 1] = Int32GetDatum(pttupcount);
    nulls[Anum_pg_pax_block_tables_pttupcount - 1] = false;
    values[Anum_pg_pax_block_tables_ptblocksize - 1] =
        Int32GetDatum(ptblocksize);
    nulls[Anum_pg_pax_block_tables_ptblocksize - 1] = false;
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

void cbdb::PaxCreateMicroPartitionTable(const Relation rel,
                                        const RelFileNode *newrnode,
                                        char persistence) {
  Relation pg_class_desc;
  char auxRelname[32];
  Oid relid;
  Oid auxRelid;
  Oid auxNamespaceId;
  Oid paxRelid;
  TupleDesc tupdesc;

  pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);
  paxRelid = RelationGetRelid(rel);

  // 1. create blocks table.
  snprintf(auxRelname, sizeof(auxRelname), "pg_pax_blocks_%u", paxRelid);
  auxNamespaceId = PG_PAXAUX_NAMESPACE;
  auxRelid = GetNewOidForRelation(pg_class_desc, ClassOidIndexId,
                                  Anum_pg_class_oid,  // new line
                                  auxRelname, auxNamespaceId);
  tupdesc = CreateTemplateTupleDesc(Natts_pg_pax_block_tables);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_pax_block_tables_ptblockname,
                     "ptblockname", NAMEOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_pax_block_tables_pttupcount,
                     "pttupcount", INT4OID, -1, 0);
  // TODO(chenhongjie): uncompressed and compressed ptblocksize are needed.
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_pax_block_tables_ptblocksize,
                     "ptblocksize", INT4OID, -1, 0);
  relid = heap_create_with_catalog(
      auxRelname, auxNamespaceId, rel->rd_rel->reltablespace, auxRelid,
      InvalidOid, InvalidOid, rel->rd_rel->relowner, HEAP_TABLE_AM_OID, tupdesc,
      NIL, RELKIND_RELATION, rel->rd_rel->relpersistence,
      rel->rd_rel->relisshared, RelationIsMapped(rel), ONCOMMIT_NOOP,
      NULL,                         /* GP Policy */
      (Datum)0, false,              /* use _user_acl */
      true, true, InvalidOid, NULL, /* typeaddress */
      false /* valid_opts */);
  Assert(relid == auxRelid);
  table_close(pg_class_desc, NoLock);

  // 2. insert entry into pg_pax_tables.
  InsertPaxTablesEntry(paxRelid, auxRelid, "", 0);

  // 3. record pg_depend, pg_pax_blocks_<xxx> depends relation.
  {
    ObjectAddress base;
    ObjectAddress aux;
    base.classId = RelationRelationId;
    base.objectId = paxRelid;
    base.objectSubId = 0;
    aux.classId = RelationRelationId;
    aux.objectId = auxRelid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);

    // pg_pax_tables single row depend
    base.classId = RelationRelationId;
    base.objectId = paxRelid;
    base.objectSubId = 0;
    aux.classId = PaxTablesRelationId;
    aux.objectId = paxRelid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
  }

  // 4. create micro-partition file directory
  paxc::CreateMicroPartitionFileDirectory(newrnode, rel->rd_backend,
                                          persistence);
}

void cbdb::GetAllBlockFileInfo_PG_PaxBlock_Relation(
    std::shared_ptr<std::vector<MicroPartitionMetadataPtr>> result,
    const Relation relation, const Relation pg_blockfile_rel,
    const Snapshot paxMetaDataSnapshot) {
  TupleDesc pg_paxblock_dsc;
  HeapTuple tuple;
  SysScanDesc pax_scan;
  Datum blockid, tup_count;
  bool is_null;

  CBDB_WRAP_START;
  {
    pg_paxblock_dsc = RelationGetDescr(pg_blockfile_rel);
    pax_scan = systable_beginscan(pg_blockfile_rel, InvalidOid, false,
                                  paxMetaDataSnapshot, 0, NULL);

    while ((tuple = systable_getnext(pax_scan)) != NULL) {
      blockid = heap_getattr(tuple, Anum_pg_pax_block_tables_ptblockname,
                             pg_paxblock_dsc, &is_null);
      if (is_null)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("got invalid segno value: NULL")));

      std::string file_name = pax::TableMetadata::BuildPaxFilePath(
          relation, DatumGetName(blockid)->data);

      std::shared_ptr<pax::MicroPartitionMetadata> meta_info =
          std::make_shared<pax::MicroPartitionMetadata>(
              DatumGetName(blockid)->data, file_name);

      tup_count = heap_getattr(tuple, Anum_pg_pax_block_tables_pttupcount,
                               pg_paxblock_dsc, &is_null);
      if (is_null)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("got invalid tupcount value: NULL")));
      meta_info->setTupleCount(tup_count);

      // TODO(gongxun): add more statistics info

      result->push_back(meta_info);
    }

    systable_endscan(pax_scan);
  }
  CBDB_WRAP_END;
}

void cbdb::GetAllMicroPartitionMetadata(
    const Relation parentrel, const Snapshot paxMetaDataSnapshot,
    std::shared_ptr<std::vector<MicroPartitionMetadataPtr>> result) {
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

    GetAllBlockFileInfo_PG_PaxBlock_Relation(result, parentrel, pg_paxblock_rel,
                                             paxMetaDataSnapshot);

    table_close(pg_paxblock_rel, AccessShareLock);
  }
  CBDB_WRAP_END;
}

void cbdb::AddMicroPartitionEntry(const pax::WriteSummary &summary) {
  Oid pax_block_tables_rel_id;
  cbdb::GetMicroPartitionEntryAttributes(summary.rel_oid,
                                         &pax_block_tables_rel_id, NULL, NULL);
  cbdb::InsertPaxBlockEntry(pax_block_tables_rel_id, summary.block_id.c_str(),
                            summary.num_tuples, summary.file_size);
}
