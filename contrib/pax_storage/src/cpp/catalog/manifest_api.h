#pragma once

/*
 * abstract interface of hashdata manifest
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
/*
 * for all ther interface function, we only handle the error by returning
 * the errorcode without throwing any exception like what postgres do, the
 * error code will be returned by pointer as specified in the argument,
 * error code variable was created by user, and set by library, and if a
 * interface was defined to return nothing, will just return error code
 * instead.
 *
 * library will utilize the postgres memorycontxt to do memory management.
 * there are two kinds of memory management level, the first is for control
 * data which will be managed under TopMemoryContext, that means the data
 * will be existed all the time with the session, the second is for
 * transaction data, like the manifest tuples, they will be under the
 * TopTransactionContext, that means all the memory will be release after
 * transaction complete or abort, the library usr need to take care the
 * memory reset work.
 *
 * TODO: define the internal key point for interface implementation
 */

/* hashdata manifest support following data types for fields */
typedef enum MetaFieldType
{
  Meta_Field_Type_Int = 1,
  Meta_Field_Type_Uint,
  Meta_Field_Type_String,
  Meta_Field_Type_Float,
  Meta_Field_Type_Bool,
  Meta_Field_Type_Bytes
} MetaFieldType;


typedef struct MetaAttribute
{
  const char *field_name;
  MetaFieldType field_type;
  Datum deflt;
} MetaAttribute;

typedef struct ManifestDescData
{
  int numattrs;
  MetaAttribute attrs[FLEXIBLE_ARRAY_MEMBER];
} ManifestDescData;

typedef ManifestDescData *ManifestDesc;

typedef struct MetaValue
{
  const char *field_name;
  Datum value;
} MetaValue;

typedef struct ManifestScanKeyData {
  MetaValue field;
} ManifestScanKeyData;

/* some forward declaration */
/*
 * define the handle returned by manifest_open for operating manifest CRUD
 */
#ifndef USE_OWN_MANIFEST_RELATION
typedef struct ManifestRelationData ManifestRelationData;
typedef ManifestRelationData *ManifestRelation;
#endif

/*
 * define the manifest tuple identifier returned by getnext and insert interface
 * the handle will be provided for update/delete operation
 */
#ifndef USE_OWN_MANIFEST_TUPLE
typedef struct ManifestTupleData ManifestTupleData;
typedef ManifestTupleData *ManifestTuple;
#endif

/*
 * define manifest scan iterator for getting next tuple
 */
#ifndef USE_OWN_MANIFEST_SCAN
typedef struct ManifestScanData ManifestScanData;
typedef ManifestScanData *ManifestScan;
#endif

/*
 * define the scan key for scanning the manifest tuple
 */
typedef struct ManifestScanKeyData ManifestScanKeyData;
typedef ManifestScanKeyData *ManifestScanKey;
typedef struct RelationData *Relation;
typedef struct SnapshotData *Snapshot;
typedef struct RelFileNode RelFileNode;


/*
 * initialize internal resources
 */
ManifestDesc manifest_init();

/*
 * create the first/empty manifest resource when creatting the table or
 * truncate the table manifest resource include the top manifest entry
 * and manifest file return error if something went wrong.
 * Argument:
 *   relnode - the relnode of the pax table which is composed with
 *             tablespaceId, ReiFileNodeId, databaseId
 */
void manifest_create(Relation rel, RelFileNode newrelnode);

/*
 * remove the manifest resource when dropping the table
 * including the top manifest entry and all the current and historical manifest files
 * return error if something went wrong.
 */
void manifest_remove(Relation rel, RelFileNode relnode);


/*
 * Clear all micro partitions in the current version in non-transaction
 * When the current version is temporary(like newly created table),
 * truncating a table doesn't require rollback.
 * This function will remove all data files in the current relnode version.
 */
void manifest_truncate(Relation rel);


/*
 * open and get the manifest operation handle
 * the memory of the handle will be handled by library, user need not to release it
 * Aguments:
 *   relnode - the relnode of the pax table which is composed with
 *             tablespaceId, ReiFileNodeId, databaseId
 *   errcode - error code returned if fail
 *
 * return value is null if something goes wrong, and need to check the errcode
 * for detail
 */
ManifestRelation manifest_open(Relation rel);

/*
 * close the opened manifest handle, the closed manifest handle cannot
 * be used anymore, if want to use a handle, please do open it again
 * 
 * error will happen when to close a ManifestRelation which is not opened any more.
 */
void manifest_close(ManifestRelation mfrel);


/*
 * init a manifest meta scan key, the memory of the key need to created by user and
 * freed by user.
 * Arguments:
 * key - struct instance to initialize
 * data - if strategy is by field value, specify the value here
 *
 * return true for succeed, and false for fail
 */
void manifest_scan_key_init(ManifestScanKey key, MetaValue data);

/*
 * create a manifest tuple scan iterator with a scan key.
 * if the key is provided with NULL, the scan will just iterate all over manifest
 * tuples the returned ManifestScanData instance is created internally, and will be
 * release when calling manifest_endscan
 * Arguments:
 * mfrel - the manifest operation handle which returned by manifest_open interface
 * key - define search key
 * errcode - error code returned if fail
 *
 * return value is null if something goes wrong, check the errcode for detail
 */
ManifestScan manifest_beginscan(ManifestRelation mfrel, Snapshot snapshot, ManifestScanKey key);

/*
 * end up a scan iterator, free the memory of the scan instance
 * return error code if fail
 */
void manifest_endscan(ManifestScan scan);

/*
 * return a manifest tuple from scan iterator, the memory for returned manifest tuple
 * allocated by library, and the memory will be released by transaction commit.
 * the interface support parallel processing, since the memory for all the manifest
 * tuples has been all allocated at the init, the only posision of the iterator was
 * protected by atomic operation.
 * Argument:
 * scan - scan iterator created by manifest_beginscan
 * errcode - error code returned if fail
 * context - a extended filter object context
 */
ManifestTuple manifest_getnext(ManifestScan scan, void *context);

/*
 * insert a new manifest tuple with a set of given name-value pairs,
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * data - an array of a name-value pair to setup a manifest tuple
 * count - the name-value array size
 * errcode - error code returned if fail
 *
 * interface return a manifest tuple instance which has been inserted
 * to manifest.
 *
 * the interface call cannot support parallel processing currently
 * the given name-value pairs will be copied to library internally, and
 * of course the user can release these space after calling this function
 * 
 * notice that the inserted tuple will be synced to disk when only after
 * call the manifest_commit.
 */
void manifest_insert(ManifestRelation mrel, const MetaValue data[],
                     int count);

/*
 * update a manifest tuple with a set of given name-value pairs,
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * data - an array of a name-value pair to setup a manifest tuple
 * count - the name-value array size
 * errcode - error code returned if fail
 *
 * interface return a manifest tuple instance which has been updated
 * to manifest. in this library, update will not directly update the old
 * record, and just create a new version of record with new values instead
 * and old version will not be seen. the transaction commit will remove the
 * invisible record finally.
 *
 * the interface call cannot support parallel processing currently
 * the given name-value pairs will be copied to library internally, and
 * of course the user can release these space after calling this function
 * 
 * notice that the inserted tuple will be synced to disk when only after
 * call the manifest_commit.
 */
void manifest_update(ManifestRelation mrel, ManifestTuple oldtuple,
                     const MetaValue data[], int count);
/*
 * delete a manifest tuple
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * tuple - manifest tuple to delete
 *
 * the interface call cannot support parallel processing currently
 * and the library actually will delete the given tuple and just
 * mark it invisible instead, the transaction commit will remove the
 * invisible record finally.
 *
 * the interface call cannot support parallel processing currently
 *
 * notice that input tuple should be instance returned from manifest_getnext
 */
void manifest_delete(ManifestRelation mrel, ManifestTuple tuple);

/*
 * commit all manifest changes, and sync manifest to disk
 * Argument:
 * relnode - the manifest identifier
 *
 * cleanup all the invisible manifest tuples, and save all visible tuples to disk.
 * also update manifest top entrance heap record with new generated file path
 * the heap table update operaiton will require a RowExeclusiveLock, which means
 * any DML query for a same table will be hold on this lock, it can be called parallelly
 * but will be executed serially for the same relnode.
 *
 * notice that all the memory that allocated during the a transaction processing for
 * manifest will be released here.
 */
void manifest_commit(Relation rel);

/*
 * get a field value from a manifest tuple by a given field name
 * Arguments:
 * tuple - manifest tuple which get from manifest interface
 * mrel - manifest operation handle
 * field_name - the field name to get the value for
 * value - the datum where value stored
 * isnull - indicate the value is null
 * errcode - error code returned if fail
 *
 * the returnd value's memory space is allocated in library, and will be released
 * by transaction_commit, caller need to to free it.
 */
Datum get_manifesttuple_value(ManifestTuple tuple, ManifestRelation mrel,
                              const char* field_name, bool *isnull);

ManifestTuple manifest_find(ManifestRelation mrel, Snapshot snapshot, int block);

/**
 * Release ManifestTuple acquired from manifest_find().
 * 
 */
void manifest_free_tuple(ManifestTuple tuple);

/**
 * Swap data between two pax tables, but not swap oids
 */
void manifest_swap_table(Oid relid1, Oid relid2,
                         TransactionId frozen_xid,
                         MultiXactId cutoff_multi);
#ifdef __cplusplus
}
#endif
