#include "pax_aux_table.h"

#include "comm/cbdb_wrappers.h"

const std::string cbdb::GenRandomBlockId() {
  CBDB_WRAP_START;
  {
    uuid_t uuid;
    uuid_generate(uuid);
    char str[36] = {0};
    uuid_unparse(uuid, str);
    std::string uuid_str = str;
    return uuid_str;
  }
  CBDB_WRAP_END;
  return nullptr;
}

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

void cbdb::InsertPaxBlockEntry(Oid relid, const char *blockname,
                               int pttupcount) {
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
    values = (Datum *)palloc(sizeof(Datum) * natts);
    nulls = (bool *)palloc0(sizeof(bool) * natts);

    Assert(blockname);
    namestrcpy(&ptblockname, blockname);

    values[Anum_pg_pax_block_tables_ptblockname - 1] =
        NameGetDatum(&ptblockname);
    values[Anum_pg_pax_block_tables_pttupcount - 1] = Int32GetDatum(pttupcount);

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
                                        const Oid relfilenode) {
  Relation pg_class_desc;
  char *blocks_relname;
  Oid blocks_relid;
  Oid relid;
  Oid blocks_namespaceId;
  TupleDesc tupdesc;

  CBDB_WRAP_START;
  {
    pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);

    // create blocks table
    blocks_relname = psprintf("pg_pax_blocks_%d", relfilenode);
    blocks_namespaceId = PG_PAXAUX_NAMESPACE;
    blocks_relid =
        GetNewOidForRelation(pg_class_desc, ClassOidIndexId, Anum_pg_class_oid,
                             blocks_relname, blocks_namespaceId);

    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "ptblockname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "pttupcount", INT4OID, -1, 0);
    relid = heap_create_with_catalog(
        blocks_relname, blocks_namespaceId, rel->rd_rel->reltablespace,
        blocks_relid, InvalidOid, InvalidOid, rel->rd_rel->relowner,
        HEAP_TABLE_AM_OID, tupdesc, NIL, 'r', rel->rd_rel->relpersistence,
        rel->rd_rel->relisshared, RelationIsMapped(rel), ONCOMMIT_NOOP, NULL,
        (Datum)0, false, true, true, InvalidOid, NULL, false);
    CommandCounterIncrement();

    // insert entry to pg_pax_tables
    InsertPaxTablesEntry(rel->rd_id, relid, "", 0);

    CommandCounterIncrement();

    table_close(pg_class_desc, NoLock);

    // record pg_depend, pg_pax_blocks_<xxx> depends relation
    {
      ObjectAddress base;
      ObjectAddress aux;
      base.classId = RelationRelationId;
      base.objectId = rel->rd_id;
      base.objectSubId = 0;
      aux.classId = RelationRelationId;
      aux.objectId = relid;
      aux.objectSubId = 0;
      recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);

      // pg_pax_tables single row depend
      base.classId = RelationRelationId;
      base.objectId = rel->rd_id;
      base.objectSubId = 0;
      aux.classId = PaxTablesRelationId;
      aux.objectId = rel->rd_id;
      aux.objectSubId = 0;
      recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
    }
  }
  CBDB_WRAP_END;
}