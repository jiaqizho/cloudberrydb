#include "catalog/pax_fastsequence.h"

namespace paxc {
static Oid pax_fastsequence_oid = InvalidOid;
static Oid pax_fastsequence_index_oid = InvalidOid;

static void CPaxUpdateFastsequence(Relation pax_fastsequence_rel,
                                   HeapTuple old_tuple,
                                   TupleDesc tuple_desc,
                                   Oid objid,
                                   int64 new_last_sequence);

static HeapTuple CPaxOpenFastSequenceTable(Oid objid,
                                           Relation *pax_fastsequence_rel,
                                           SysScanDesc *scan,
                                           LOCKMODE lock_mode);

static void CPaxCloseFastSequenceTable(Relation pax_fastsequence_rel,
                                       SysScanDesc scan,
                                       LOCKMODE lock_mode);

// InsertInitialFastSequenceEntries is used to generate and keep track of allocated micropartition file number.
// objid indicates single pax micro-partition table oid.
// lastsequence indicates the current allocated file number by using fastsequence allocation.
void CPaxInsertInitialFastSequenceEntries(Oid objid) {
  Relation pax_fastsequence_rel;
  TupleDesc tuple_desc;
  HeapTuple tuple = NULL;
  Datum values[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];
  bool nulls[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];

  // Open and lock the pax_fastsequence catalog table.
  if (!OidIsValid(pax_fastsequence_oid))
    pax_fastsequence_oid = get_relname_relid(PG_PAX_FAST_SEQUENCE_TABLE, PG_PAXAUX_NAMESPACE);

  // Initilize a new object id and use row-based exclusive lock to avoid concurrency issue.
  pax_fastsequence_rel = table_open(pax_fastsequence_oid, RowExclusiveLock);
  tuple_desc = RelationGetDescr(pax_fastsequence_rel);

  values[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = ObjectIdGetDatum(objid);
  values[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = Int64GetDatum(0);

  tuple = heaptuple_form_to(tuple_desc, values, nulls, NULL, NULL);
  CatalogTupleInsert(pax_fastsequence_rel, tuple);
  heap_freetuple(tuple);

  table_close(pax_fastsequence_rel, RowExclusiveLock);
}

// Get the required objid Tuple from pg_pax_fastsequence system table. 
// objid indicates single pax micro-partition table oid.
// lock_mode indicates the lock level used when retrive data from system table.
static HeapTuple CPaxOpenFastSequenceTable(Oid objid,
                                          Relation *pax_fastsequence_rel,
                                          SysScanDesc *pax_fastsequece_scan,
                                          LOCKMODE lock_mode) {
  ScanKeyData scankey[1];
  HeapTuple tuple;
  Relation rel;
  SysScanDesc scan;

  if (!OidIsValid(pax_fastsequence_oid))
    pax_fastsequence_oid = get_relname_relid(PG_PAX_FAST_SEQUENCE_TABLE, PG_PAXAUX_NAMESPACE);
  if (!OidIsValid(pax_fastsequence_index_oid))
    pax_fastsequence_index_oid = get_relname_relid(PG_PAX_FAST_SEQUENCE_INDEX_NAME, PG_PAXAUX_NAMESPACE);

  Assert(OidIsValid(pax_fastsequence_oid) && OidIsValid(pax_fastsequence_index_oid));

  rel = table_open(pax_fastsequence_oid, lock_mode);

  /* SELECT * FROM paxaux.pg_pax_fastsequence WHERE objid = :1 FOR UPDATE */
  ScanKeyInit(&scankey[0],
              ANUM_PG_PAX_FAST_SEQUENCE_OBJID,
              BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(objid));

  scan = systable_beginscan(rel, pax_fastsequence_index_oid, true,
                            NULL, 1, scankey);

  tuple = systable_getnext(scan);

  *pax_fastsequence_rel = rel;
  *pax_fastsequece_scan = scan;

  return tuple;
}

static void CPaxCloseFastSequenceTable(Relation pax_fastsequence_rel,
                                          SysScanDesc pax_fastsequece_scan,
                                          LOCKMODE lock_mode) {
  systable_endscan(pax_fastsequece_scan);
  table_close(pax_fastsequence_rel, lock_mode);
}

// update the existing fast sequence number for (objid).
// This tuple is updated with the new value. Otherwise, a new tuple is inserted into the table.
static void CPaxUpdateFastsequence(Relation pax_fastsequence_rel,
                                   HeapTuple old_tuple,
                                   TupleDesc tuple_desc,
                                   Oid objid,
                                   int64 new_last_sequence) {
  HeapTuple new_tuple;
  Datum values[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];
  bool nulls[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];

  // If such a tuple does not exist, insert a new one.
  Assert(HeapTupleIsValid(old_tuple));

  values[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = ObjectIdGetDatum(objid);
  values[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = Int64GetDatum(new_last_sequence);

  new_tuple = heap_form_tuple(tuple_desc, values, nulls);
  Assert(HeapTupleIsValid(new_tuple));

  new_tuple->t_data->t_ctid = old_tuple->t_data->t_ctid;
  new_tuple->t_self = old_tuple->t_self;

  heap_inplace_update(pax_fastsequence_rel, new_tuple);
  heap_freetuple(new_tuple);
}

 // InsertFastSequenceEntry
 // Insert a new fast sequence entry for a given object. If the given object already exists in the table, this function replaces the old
 // entry with a fresh initial value.
void CPaxInsertFastSequenceEntry(Oid objid, int64 last_sequence) {
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  TupleDesc tuple_desc;
  HeapTuple tuple;

  // Insert a new object entry and use row-based exclusive lock to avoid concurrency issue.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan, RowExclusiveLock);

  Assert(HeapTupleIsValid(tuple));

  tuple_desc = RelationGetDescr(pax_fastsequence_rel);

  CPaxUpdateFastsequence(pax_fastsequence_rel, tuple,
                         tuple_desc, objid, last_sequence);

  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, RowExclusiveLock);
}

 // GetFastSequences
 // Get consecutive sequence numbers, the returned sequence number is the lastsequence + 1
int64 CPaxGetFastSequences(Oid objid) {
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  TupleDesc tuple_desc;
  HeapTuple tuple;
  Datum last_sequence_datum;
  int64 new_last_sequence = 0;
  bool isnull = false;

  // Increase and read sequence number base on objid and use row-based exclusive lock to avoid concurrency issue.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan, RowExclusiveLock);

  Assert(HeapTupleIsValid(tuple));

  tuple_desc = RelationGetDescr(pax_fastsequence_rel);

  last_sequence_datum = heap_getattr(tuple, ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE, tuple_desc, &isnull);
  if (isnull) {
    ereport(ERROR,
           (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("CPaxGetFastSequences got an invalid lastsequence number: NULL")));
  }
  new_last_sequence = DatumGetInt64(last_sequence_datum) + 1;

  CPaxUpdateFastsequence(pax_fastsequence_rel, tuple, tuple_desc,
                         objid, new_last_sequence);

  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, RowExclusiveLock);

  return new_last_sequence;
}

 // ReadLastSequence
 // Read the last_sequence attribute from pax_fastsequence by obiid.
 // If there is not such an entry for objid in the table, return 0.
int64 CPaxReadLastSequence(Oid objid) {
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  TupleDesc tuple_desc;
  HeapTuple tuple;
  int64 last_sequence = 0;
  bool isnull = false;

  // Read last sequence number and use AccessShareLock.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan, AccessShareLock);

  Assert(HeapTupleIsValid(tuple));

  tuple_desc = RelationGetDescr(pax_fastsequence_rel);

  last_sequence = heap_getattr(tuple, ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE, tuple_desc, &isnull);
  if (isnull) {
    ereport(ERROR,
           (errcode(ERRCODE_UNDEFINED_OBJECT),
           errmsg("got an invalid lastsequence number: NULL")));
  }

  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, AccessShareLock);

  return last_sequence;
}

// RemoveFastSequenceEntry
// Remove all entries associated with the given object id.
// If the given objid is an invalid OID, this function simply returns.
// It is okay for the given valid objid to have no entries in pax_fastsequence.
void CPaxRemoveFastSequenceEntry(Oid objid) {
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  HeapTuple tuple;

  if (!OidIsValid(objid))
    return;

  // Remove specific object id and use row-based exclusive lock to avoid concurrency issue.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan, RowExclusiveLock);

  if (HeapTupleIsValid(tuple)) {
    CatalogTupleDelete(pax_fastsequence_rel, &tuple->t_self);
  }

  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, RowExclusiveLock);
}
} // namespace

