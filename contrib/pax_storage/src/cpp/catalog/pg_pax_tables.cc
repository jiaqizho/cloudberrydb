#include "catalog/pg_pax_tables.h"

#include "comm/cbdb_api.h"

namespace paxc {

void InsertPaxTablesEntry(Oid relid, Oid blocksrelid) {
  Relation rel;
  TupleDesc desc;
  HeapTuple tuple;
  bool nulls[NATTS_PG_PAX_TABLES];
  Datum values[NATTS_PG_PAX_TABLES];

  rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  desc = RelationGetDescr(rel);
  Assert(desc->natts == NATTS_PG_PAX_TABLES);

  values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid);
  values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = ObjectIdGetDatum(blocksrelid);
  nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
  nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

  tuple = heap_form_tuple(desc, values, nulls);

  /* insert a new tuple */
  CatalogTupleInsert(rel, tuple);

  table_close(rel, NoLock);
}

void GetPaxTablesEntryAttributes(Oid relid, Oid *blocksrelid) {
  Relation rel;
  ScanKeyData key[1];
  SysScanDesc scan;
  HeapTuple tuple;
  bool isnull;

  rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);

  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid));

  scan = systable_beginscan(rel, PAX_TABLES_RELID_INDEX_ID, true, NULL, 1, key);
  tuple = systable_getnext(scan);
  if (!HeapTupleIsValid(tuple))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("pax table relid \"%d\" does not exist in "
                           "pg_pax_tables",
                           relid)));

  if (blocksrelid) {
    *blocksrelid = heap_getattr(tuple, ANUM_PG_PAX_TABLES_AUXRELID,
                                RelationGetDescr(rel), &isnull);
    if (isnull) ereport(ERROR, (errmsg("pg_pax_tables.auxrelid is null")));
  }

  /* Finish up scan and close pg_pax_tables catalog. */
  systable_endscan(scan);
  table_close(rel, NoLock);
}

}  // namespace paxc
