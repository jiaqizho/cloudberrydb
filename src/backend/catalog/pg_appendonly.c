/*-------------------------------------------------------------------------
 *
 * pg_appendonly.c
 *	  routines to support manipulation of the pg_appendonly relation
 *
 * Portions Copyright (c) 2008, Greenplum Inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/catalog/pg_appendonly.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "catalog/pg_am_d.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/gp_fastsequence.h"
#include "access/appendonlywriter.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute_encoding.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "catalog/gp_indexing.h"


static void TransferAppendonlyEntries(Oid fromrelid, Oid torelid);

/*
 * Adds an entry into the pg_appendonly catalog table. The entry
 * includes the new relfilenode of the appendonly relation that 
 * was just created and an initial eof and reltuples values of 0
 */
void
InsertAppendOnlyEntry(Oid relid, 
					  int blocksize, 
					  int compresslevel,
					  bool checksum,
                      bool columnstore,
					  char* compresstype,
					  Oid segrelid,
					  Oid blkdirrelid,
					  Oid blkdiridxid,
					  Oid visimaprelid,
					  Oid visimapidxid,
					  int16 version)
{
	Relation	pg_appendonly_rel;
	HeapTuple	pg_appendonly_tuple = NULL;
	NameData	compresstype_name;
	bool	   *nulls;
	Datum	   *values;
	int			natts = 0;

    /*
     * Open and lock the pg_appendonly catalog.
     */
	pg_appendonly_rel = table_open(AppendOnlyRelationId, RowExclusiveLock);

	natts = Natts_pg_appendonly;
	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);

	/*
	 * GPDB_12_MERGE_FIXME:
	 *		Consider not storing the parsed values for blocksize, compresstype,
	 *		compresslevel and checksum as those are also present in StdRdOptions
	 *		of Relcache.
	 */

	if (compresstype)
		namestrcpy(&compresstype_name, compresstype);
	else
		namestrcpy(&compresstype_name, "");
	values[Anum_pg_appendonly_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_appendonly_blocksize - 1] = Int32GetDatum(blocksize);
	values[Anum_pg_appendonly_compresslevel - 1] = Int32GetDatum(compresslevel);
	values[Anum_pg_appendonly_checksum - 1] = BoolGetDatum(checksum);
	values[Anum_pg_appendonly_compresstype - 1] = NameGetDatum(&compresstype_name);
	values[Anum_pg_appendonly_columnstore - 1] = BoolGetDatum(columnstore);
	values[Anum_pg_appendonly_segrelid - 1] = ObjectIdGetDatum(segrelid);
	values[Anum_pg_appendonly_segfilecount- 1] = Int16GetDatum(0);
	values[Anum_pg_appendonly_version - 1] = Int16GetDatum(version);
	values[Anum_pg_appendonly_blkdirrelid - 1] = ObjectIdGetDatum(blkdirrelid);
	values[Anum_pg_appendonly_blkdiridxid - 1] = ObjectIdGetDatum(blkdiridxid);
	values[Anum_pg_appendonly_visimaprelid - 1] = ObjectIdGetDatum(visimaprelid);
	values[Anum_pg_appendonly_visimapidxid - 1] = ObjectIdGetDatum(visimapidxid);
	

	/*
	 * form the tuple and insert it
	 */
	pg_appendonly_tuple = heap_form_tuple(RelationGetDescr(pg_appendonly_rel), values, nulls);

	/* insert a new tuple */
	CatalogTupleInsert(pg_appendonly_rel, pg_appendonly_tuple);

	/*
     * Close the pg_appendonly_rel relcache entry without unlocking.
     * We have updated the catalog: consequently the lock must be 
	 * held until end of transaction.
     */
    table_close(pg_appendonly_rel, NoLock);

	pfree(values);
	pfree(nulls);

}

void
GetAppendOnlyEntryAttributes(Oid relid,
							 int32 *blocksize,
							 int16 *compresslevel,
							 bool *checksum,
							 NameData *compresstype)
{
	Relation	pg_appendonly;
	TupleDesc	tupDesc;
	ScanKeyData	key[1];
	SysScanDesc	scan;
	HeapTuple	tuple;
	Form_pg_appendonly	aoForm;

	pg_appendonly = table_open(AppendOnlyRelationId, AccessShareLock);
	tupDesc = RelationGetDescr(pg_appendonly);

	ScanKeyInit(&key[0],
				Anum_pg_appendonly_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_appendonly, AppendOnlyRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_appendonly entry for relation \"%s\"",
						get_rel_name(relid))));

	aoForm = (Form_pg_appendonly) GETSTRUCT(tuple);
	if (blocksize != NULL)
		*blocksize = aoForm->blocksize;

	if (compresslevel != NULL)
		*compresslevel = aoForm->compresslevel;

	if (checksum != NULL)
		*checksum = aoForm->checksum;

	if (compresstype != NULL)
		namestrcpy(compresstype, NameStr(aoForm->compresstype));

	/* Finish up scan and close pg_appendonly catalog. */
	systable_endscan(scan);
	table_close(pg_appendonly, AccessShareLock);
}

/*
 * Get the OIDs of the auxiliary relations and their indexes for an appendonly
 * relation. This should only be called on tables with pg_appendonly entries,
 * which currently are just non-partitioned AO/CO tables.
 *
 * The OIDs will be retrieved only when the corresponding output variable is
 * not NULL.
 */
void
GetAppendOnlyEntryAuxOids(Relation rel,
						  Oid *segrelid,
						  Oid *blkdirrelid,
						  Oid *blkdiridxid,
						  Oid *visimaprelid,
						  Oid *visimapidxid)
{
	Form_pg_appendonly	aoForm;

	/* the relation has to be a non-partitioned AO/CO table */
	Assert(RelationStorageIsAO(rel));

	aoForm = rel->rd_appendonly;

	if (segrelid != NULL)
		*segrelid = aoForm->segrelid;

	if (blkdirrelid != NULL)
		*blkdirrelid = aoForm->blkdirrelid;

	if (blkdiridxid != NULL)
		*blkdiridxid = aoForm->blkdiridxid;

	if (visimaprelid != NULL)
		*visimaprelid = aoForm->visimaprelid;

	if (visimapidxid != NULL)
		*visimapidxid = aoForm->visimapidxid;
}

/*
 * Get the pg_appendonly entry for the relation. This should only be called on 
 * tables with pg_appendonly entries, which currently are just non-partitioned
 * AO/CO tables. The pg_appendonly data is copied into the Form_pg_appendonly
 * pointer which should be valid.
 */
void
GetAppendOnlyEntry(Relation rel, Form_pg_appendonly aoEntry)
{
	Form_pg_appendonly	aoForm;

	/* the relation has to be a non-partitioned AO/CO table and the aoEntry is valid */
	Assert(RelationStorageIsAO(rel));
	Assert(aoEntry);

	aoForm = rel->rd_appendonly;

	memcpy(aoEntry, aoForm, APPENDONLY_TUPLE_SIZE);
}

/*
 * Update the segrelid and/or blkdirrelid if the input new values
 * are valid OIDs.
 */
void
UpdateAppendOnlyEntryAuxOids(Oid relid,
							 Oid newSegrelid,
							 Oid newBlkdirrelid,
							 Oid newBlkdiridxid,
							 Oid newVisimaprelid,
							 Oid newVisimapidxid)
{
	Relation	pg_appendonly;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple, newTuple;
	Datum		newValues[Natts_pg_appendonly];
	bool		newNulls[Natts_pg_appendonly];
	bool		replace[Natts_pg_appendonly];
	
	/*
	 * Check the pg_appendonly relation to be certain the ao table 
	 * is there. 
	 */
	pg_appendonly = table_open(AppendOnlyRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_appendonly_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_appendonly, AppendOnlyRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_appendonly entry for relation \"%s\"",
						get_rel_name(relid))));

	MemSet(newValues, 0, sizeof(newValues));
	MemSet(newNulls, false, sizeof(newNulls));
	MemSet(replace, false, sizeof(replace));

	if (OidIsValid(newSegrelid))
	{
		replace[Anum_pg_appendonly_segrelid - 1] = true;
		newValues[Anum_pg_appendonly_segrelid - 1] = newSegrelid;
	}

	if (OidIsValid(newBlkdirrelid))
	{
		replace[Anum_pg_appendonly_blkdirrelid - 1] = true;
		newValues[Anum_pg_appendonly_blkdirrelid - 1] = newBlkdirrelid;
	}
	
	if (OidIsValid(newBlkdiridxid))
	{
		replace[Anum_pg_appendonly_blkdiridxid - 1] = true;
		newValues[Anum_pg_appendonly_blkdiridxid - 1] = newBlkdiridxid;
	}
	
	if (OidIsValid(newVisimaprelid))
	{
		replace[Anum_pg_appendonly_visimaprelid - 1] = true;
		newValues[Anum_pg_appendonly_visimaprelid - 1] = newVisimaprelid;
	}
	
	if (OidIsValid(newVisimapidxid))
	{
		replace[Anum_pg_appendonly_visimapidxid - 1] = true;
		newValues[Anum_pg_appendonly_visimapidxid - 1] = newVisimapidxid;
	}
	newTuple = heap_modify_tuple(tuple, RelationGetDescr(pg_appendonly),
								 newValues, newNulls, replace);
	CatalogTupleUpdate(pg_appendonly, &newTuple->t_self, newTuple);

	heap_freetuple(newTuple);

	/* Finish up scan and close appendonly catalog. */
	systable_endscan(scan);
	table_close(pg_appendonly, RowExclusiveLock);

	/* Also cause flush the relcache entry for the parent relation. */
	CacheInvalidateRelcacheByRelid(relid);
}

/*
 * Remove all pg_appendonly entries that the table we are DROPing
 * refers to (using the table's relfilenode)
 *
 * The gp_fastsequence entries associate with the table is also
 * deleted here.
 */
void
RemoveAppendonlyEntry(Oid relid)
{
	Relation	pg_appendonly_rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid aosegrelid = InvalidOid;
	
	/*
	 * now remove the pg_appendonly entry 
	 */
	pg_appendonly_rel = table_open(AppendOnlyRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_appendonly_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_appendonly_rel, AppendOnlyRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("appendonly table relid \"%d\" does not exist in "
						"pg_appendonly", relid)));
	
	{
		bool isNull;
		Datum datum = heap_getattr(tuple,
								   Anum_pg_appendonly_segrelid,
								   RelationGetDescr(pg_appendonly_rel),
								   &isNull);

		Assert(!isNull);
		aosegrelid = DatumGetObjectId(datum);
		Assert(OidIsValid(aosegrelid));
	}

	/* Piggyback here to remove gp_fastsequence entries */
	RemoveFastSequenceEntry(aosegrelid);

	/*
	 * Delete the appendonly table entry from the catalog (pg_appendonly).
	 */
	simple_heap_delete(pg_appendonly_rel, &tuple->t_self);
	
	/* Finish up scan and close appendonly catalog. */
	systable_endscan(scan);
	table_close(pg_appendonly_rel, NoLock);
}

/*
 * Does 2 things:
 * 	Sever existing dependencies: oid -> *
 * 	Create a new dependency: oid -> baseOid
 */
static void
TransferDependencyLink(
	Oid baseOid, 
	Oid oid,
	const char *tabletype)
{
	ObjectAddress 	baseobject;
	ObjectAddress	newobject;
	long			count;

	MemSet(&baseobject, 0, sizeof(ObjectAddress));
	MemSet(&newobject, 0, sizeof(ObjectAddress));

	Assert(OidIsValid(baseOid));
	Assert(OidIsValid(oid));

	/* Delete old dependency */
	count = deleteDependencyRecordsFor(RelationRelationId, oid, false);
	if (count != 1)
		elog(LOG, "expected one dependency record for %s table, oid %u, found %ld",
			 tabletype, oid, count);

	/* Register new dependencies */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = baseOid;
	newobject.classId = RelationRelationId;
	newobject.objectId = oid;
	
	recordDependencyOn(&newobject, &baseobject, DEPENDENCY_INTERNAL);
}

static HeapTuple 
GetAppendEntryForMove(
	Relation	pg_appendonly_rel,
	TupleDesc	pg_appendonly_dsc,
	Oid 		relId,
	Oid 		*aosegrelid,
	Oid 		*aoblkdirrelid,
	Oid         *aovisimaprelid)
{
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	bool		isNull;

	ScanKeyInit(&key[0],
				Anum_pg_appendonly_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));

	scan = systable_beginscan(pg_appendonly_rel, AppendOnlyRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(scan);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("pg_appendonly tuple not found for relation: %u", relId)));
	}

    *aosegrelid = heap_getattr(tuple,
							   Anum_pg_appendonly_segrelid,
							   pg_appendonly_dsc,
							   &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid segrelid value: NULL")));	

    *aoblkdirrelid = heap_getattr(tuple,
								  Anum_pg_appendonly_blkdirrelid,
								  pg_appendonly_dsc,
								  &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid blkdirrelid value: NULL")));	
 
	*aovisimaprelid = heap_getattr(tuple,
								   Anum_pg_appendonly_visimaprelid,
								   pg_appendonly_dsc,
								   &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid visimaprelid value: NULL")));	

	tuple = heap_copytuple(tuple);

	/* Finish up the scan. */
	systable_endscan(scan);

	return tuple;
}

static void
swapAppendonlyEntriesUsingTAM(Form_pg_class relform1, Form_pg_class relform2, TransactionId frozenXid, MultiXactId cutoffMulti)
{
	/*
	 * Swap auxiliary tables if the table AM has non-standard structure.
	 * See the details of the callback swap_relation_files.
	 */
	if (relform1->relkind == RELKIND_RELATION ||
		relform1->relkind == RELKIND_MATVIEW)
	{
		const TableAmRoutine *tam;
		Oid relam;
		relam = relform1->relam;
		if (relam != relform2->relam)
			elog(ERROR, "can't swap relation files for different AM");
		tam = GetTableAmRoutineByAmId(relam);
		if (tam->swap_relation_files)
			tam->swap_relation_files(relform1->oid, relform2->oid, frozenXid, cutoffMulti);
	}
}

/*
 * This function acts as a controller of catalog actions that needs to be
 * performed when we have an AT involving AO/AOCO table, where the original
 * table could be rewritten.
 *
 * Parameters:
 * relform1: original table that is the target of the AT operation.
 * relform2: newly created temp table that rows have been CTASed into.
 *
 * The actions vary on a case-by-case basis:
 * 1. If we are not changing the table's AM, we need to swap the pg_appendonly
 *	entries between the temp table and the original table and rewire aux
 *	table dependencies.
 * 2. If we are changing the table's AM, we need to transfer the pg_appendonly
 *	entry of one table to the other and rewire aux table dependencies. See
 *	individual case bodies for more details.
 */
void
ATAOEntries(Form_pg_class relform1, Form_pg_class relform2, 
			TransactionId frozenXid, MultiXactId cutoffMulti)
{
	switch(relform1->relam)
	{
		case HEAP_TABLE_AM_OID:
			switch(relform2->relam)
			{
				case AO_ROW_TABLE_AM_OID:
					/*
					 * Since the newly created AO table temp relid will be
					 * dropped from the catalog (later on in finish_heap_swap()),
					 * we ensure that the:
					 * 1. newly created pg_appendonly row carries the original
					 * 		heap relid.
					 * 2. newly created AO aux tables depend on the original
					 * 		heap relid.
					 */
					TransferAppendonlyEntries(relform2->oid, relform1->oid);
					break;
				case AO_COLUMN_TABLE_AM_OID:
					TransferAppendonlyEntries(relform2->oid, relform1->oid);
					break;
				case HEAP_TABLE_AM_OID:
				default:
					Assert(false);
			}
			break;
		case AO_ROW_TABLE_AM_OID:
			switch(relform2->relam)
			{
				case HEAP_TABLE_AM_OID:
					/*
					 * Since the newly created heap table temp relid will be
					 * dropped from the catalog (later on in finish_heap_swap()),
					 * we ensure that the:
					 * 1. original AO table's pg_appendonly row carries the heap
					 * 		temp table's relid.
					 * 2. original AO table's AO aux tables now depend on the
					 * 		heap temp table's relid.
					 * This way when the heap temp table relid is dropped from
					 * the catalog, the pg_appendonly row and aux tables also
					 * follow suit.
					 */
					TransferAppendonlyEntries(relform1->oid, relform2->oid);
					break;
				case AO_ROW_TABLE_AM_OID:
					swapAppendonlyEntriesUsingTAM(relform1, relform2, frozenXid, cutoffMulti);
					break;
				case AO_COLUMN_TABLE_AM_OID:
					SwapAppendonlyEntries(relform1->oid, relform2->oid);
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("alter table does not support switch from AO to given access method")));
			}
			break;
		case AO_COLUMN_TABLE_AM_OID:
			switch(relform2->relam)
			{
				case HEAP_TABLE_AM_OID:
					/* For pg_appendonly entries, it's the same as AO->Heap. */
					TransferAppendonlyEntries(relform1->oid, relform2->oid);
					/* Remove the pg_attribute_encoding entries, since heap tables shouldn't have these. */
					RemoveAttributeEncodingsByRelid(relform1->oid);
					break;
				case AO_ROW_TABLE_AM_OID:
					/* For pg_appendonly entries, it's same as AO->AO/CO. */
					SwapAppendonlyEntries(relform1->oid, relform2->oid);
					/* For pg_attribute_encoding entries, it's same as AOCO->heap.*/
					RemoveAttributeEncodingsByRelid(relform1->oid);
					break;
				case AO_COLUMN_TABLE_AM_OID:
					swapAppendonlyEntriesUsingTAM(relform1, relform2, frozenXid, cutoffMulti);
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("alter table does not support switch from AOCO to given access method")));
			}
			break;
	}
}

/*
 * Transfer the pg_appendonly_entry of the relation identified by fromrelid to
 * the relation identified by torelid.
 *
 * This is done by simply overwriting the relid field of the pg_appendonly row
 * for fromrelid with value = torelid.
 *
 * For completeness, also rewire the aux table dependencies to point to torelid.
 */
static void
TransferAppendonlyEntries(Oid fromrelid, Oid torelid)
{
	Relation	pg_appendonly_rel;
	TupleDesc	pg_appendonly_dsc;
	HeapTuple	pg_appendonly_tuple;
	Datum 		*newValues;
	bool 		*newNulls;
	bool 		*replace;
	Oid			aosegrelid;
	Oid			aoblkdirrelid;
	Oid			aovisimaprelid;

	pg_appendonly_rel = table_open(AppendOnlyRelationId, RowExclusiveLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);

	pg_appendonly_tuple = GetAppendEntryForMove(
		pg_appendonly_rel,
		pg_appendonly_dsc,
		fromrelid,
		&aosegrelid,
		&aoblkdirrelid,
		&aovisimaprelid);

	newValues = palloc0(pg_appendonly_dsc->natts * sizeof(Datum));
	newNulls = palloc0(pg_appendonly_dsc->natts * sizeof(bool));
	replace = palloc0(pg_appendonly_dsc->natts * sizeof(bool));

	replace[Anum_pg_appendonly_relid - 1] = true;
	newValues[Anum_pg_appendonly_relid - 1] = torelid;

	pg_appendonly_tuple = heap_modify_tuple(pg_appendonly_tuple, pg_appendonly_dsc,
								   newValues, newNulls, replace);

	CatalogTupleUpdate(pg_appendonly_rel, &pg_appendonly_tuple->t_self, pg_appendonly_tuple);

	heap_freetuple(pg_appendonly_tuple);

	table_close(pg_appendonly_rel, NoLock);

	pfree(newValues);
	pfree(newNulls);
	pfree(replace);

	if (OidIsValid(aosegrelid))
		TransferDependencyLink(torelid, aosegrelid, "aoseg");
	if (OidIsValid(aoblkdirrelid))
		TransferDependencyLink(torelid, aoblkdirrelid, "aoblkdir");
	if (OidIsValid(aovisimaprelid))
		TransferDependencyLink(torelid, aovisimaprelid, "aovisimap");
}

/*
 * Swap pg_appendonly entries between tables and transfer aux table dependencies.
 */
void
SwapAppendonlyEntries(Oid entryRelId1, Oid entryRelId2)
{
	Relation	pg_appendonly_rel;
	TupleDesc	pg_appendonly_dsc;
	HeapTuple	tupleCopy1;
	HeapTuple	tupleCopy2;
	Datum 		*newValues;
	bool 		*newNulls;
	bool 		*replace;
	Oid			aosegrelid1;
	Oid			aoblkdirrelid1;
	Oid			aovisimaprelid1;
	Oid			aosegrelid2;
	Oid			aoblkdirrelid2;
	Oid			aovisimaprelid2;

	pg_appendonly_rel = table_open(AppendOnlyRelationId, RowExclusiveLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);
	
	tupleCopy1 = GetAppendEntryForMove(
							pg_appendonly_rel,
							pg_appendonly_dsc,
							entryRelId1,
							&aosegrelid1,
							&aoblkdirrelid1,
							&aovisimaprelid1);

	tupleCopy2 = GetAppendEntryForMove(
							pg_appendonly_rel,
							pg_appendonly_dsc,
							entryRelId2,
							&aosegrelid2,
							&aoblkdirrelid2,
							&aovisimaprelid2);

	/* Since gp_fastsequence entry is referenced by aosegrelid, it rides along  */
	simple_heap_delete(pg_appendonly_rel, &tupleCopy1->t_self);
	simple_heap_delete(pg_appendonly_rel, &tupleCopy2->t_self);

	/*
	 * (Re)insert.
	 */
	newValues = palloc0(pg_appendonly_dsc->natts * sizeof(Datum));
	newNulls = palloc0(pg_appendonly_dsc->natts * sizeof(bool));
	replace = palloc0(pg_appendonly_dsc->natts * sizeof(bool));

	replace[Anum_pg_appendonly_relid - 1] = true;
	newValues[Anum_pg_appendonly_relid - 1] = entryRelId2;

	tupleCopy1 = heap_modify_tuple(tupleCopy1, pg_appendonly_dsc,
								  newValues, newNulls, replace);

	CatalogTupleInsert(pg_appendonly_rel, tupleCopy1);

	heap_freetuple(tupleCopy1);

	newValues[Anum_pg_appendonly_relid - 1] = entryRelId1;

	tupleCopy2 = heap_modify_tuple(tupleCopy2, pg_appendonly_dsc,
								  newValues, newNulls, replace);

	CatalogTupleInsert(pg_appendonly_rel, tupleCopy2);

	heap_freetuple(tupleCopy2);

	table_close(pg_appendonly_rel, NoLock);

	pfree(newValues);
	pfree(newNulls);
	pfree(replace);

	if ((aosegrelid1 || aosegrelid2) && (aosegrelid1 != aosegrelid2))
	{
		if (OidIsValid(aosegrelid1))
		{
			TransferDependencyLink(entryRelId2, aosegrelid1, "aoseg");
		}
		if (OidIsValid(aosegrelid2))
		{
			TransferDependencyLink(entryRelId1, aosegrelid2, "aoseg");
		}
	}
	
	if ((aoblkdirrelid1 || aoblkdirrelid2) && (aoblkdirrelid1 != aoblkdirrelid2))
	{
		if (OidIsValid(aoblkdirrelid1))
		{
			TransferDependencyLink(entryRelId2, aoblkdirrelid1, "aoblkdir");
		}
		if (OidIsValid(aoblkdirrelid2))
		{
			TransferDependencyLink(entryRelId1, aoblkdirrelid2, "aoblkdir");
		}
	}
	
	if ((aovisimaprelid1 || aovisimaprelid2) && (aovisimaprelid1 != aovisimaprelid2))
	{
		if (OidIsValid(aovisimaprelid1))
		{
			TransferDependencyLink(entryRelId2, aovisimaprelid1, "aovisimap");
		}
		if (OidIsValid(aovisimaprelid2))
		{
			TransferDependencyLink(entryRelId1, aovisimaprelid2, "aovisimap");
		}
	}
}

int16
GetAppendOnlySegmentFilesCount(Relation rel)
{
	Relation	pg_aoseg_rel;
	HeapTuple	tuple;
	SysScanDesc aoscan;
	int16		result = 0;
	Oid segrelid = InvalidOid;

	GetAppendOnlyEntryAuxOids(rel, &segrelid, NULL,
			NULL, NULL, NULL);
	if (segrelid == InvalidOid)
		elog(ERROR, "could not find pg_aoseg aux table for AO table \"%s\"",
			 RelationGetRelationName(rel));

	pg_aoseg_rel = table_open(segrelid, AccessShareLock);
	aoscan = systable_beginscan(pg_aoseg_rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(aoscan)) != NULL)
	{
		result++;
		CHECK_FOR_INTERRUPTS();
	}
	Assert(result <= MAX_AOREL_CONCURRENCY);
	systable_endscan(aoscan);
	table_close(pg_aoseg_rel, AccessShareLock);
	return result;
}

int16
AORelationVersion_Get(Relation rel)
{
	FormData_pg_appendonly			aoFormData;
	
	GetAppendOnlyEntry(rel, &aoFormData);

	return aoFormData.version;
}

bool
AORelationVersion_Validate(Relation rel, int16 version)
{
	return AORelationVersion_Get(rel) >= version;
}
