#include "postgres.h" // NOLINT

#include "access/tableam.h"
#include "access/skey.h"
#include "access/heapam.h"
#include "commands/vacuum.h"
#include "nodes/tidbitmap.h"
#include "nodes/execnodes.h"
#include "catalog/index.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);

static const TupleTableSlotOps * pax_slot_callbacks(Relation rel) {
    return &TTSOpsVirtual;
}

static TableScanDesc
pax_beginscan_extractcolumns(Relation rel, Snapshot snapshot,
                              List *targetlist, List *qual,
                              uint32 flags) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static TableScanDesc
pax_beginscan_extractcolumns_bm(Relation rel, Snapshot snapshot,
                                 List *targetlist, List *qual,
                                 List *bitmapqualorig,
                                 uint32 flags) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


/*
 * This function intentionally ignores key and nkeys
 */
static TableScanDesc
pax_beginscan(Relation relation,
               Snapshot snapshot,
               int nkeys, struct ScanKeyData *key,
               ParallelTableScanDesc pscan,
               uint32 flags) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_endscan(TableScanDesc scan) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_rescan(TableScanDesc scan, ScanKey key,
                  bool set_params, bool allow_strat,
                  bool allow_sync, bool allow_pagemode) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static Size
pax_parallelscan_estimate(Relation rel) {
    elog(ERROR, "parallel SeqScan not implemented for AO_COLUMN tables");
}

static Size
pax_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan) {
    elog(ERROR, "parallel SeqScan not implemented for AO_COLUMN tables");
}

static void
pax_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan) {
    elog(ERROR, "parallel SeqScan not implemented for AO_COLUMN tables");
}



static IndexFetchTableData *
pax_index_fetch_begin(Relation rel) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_index_fetch_reset(IndexFetchTableData *scan) {
    /*
     * Unlike Heap, we don't release the resources(fetch descriptor and its
     * members) here because it is more like a global data structure shared
     * across scans, rather than an iterator to yield a granularity of data.
     * 
     * Additionally, should be aware of that no matter whether allocation or
     * release on fetch descriptor, it is considerably expensive.
     */
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_index_fetch_end(IndexFetchTableData *scan) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_index_fetch_tuple(struct IndexFetchTableData *scan,
                             ItemPointer tid,
                             Snapshot snapshot,
                             TupleTableSlot *slot,
                             bool *call_again, bool *all_dead) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                        int options, BulkInsertState bistate) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                    CommandId cid, int options,
                                    BulkInsertState bistate, uint32 specToken) {
    /* GPDB_12_MERGE_FIXME: not supported. Can this function be left out completely? Or ereport()? */
    elog(ERROR, "speculative insertion not supported on PAX tables");
}

static void
pax_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                      uint32 specToken, bool succeeded) {
    elog(ERROR, "speculative insertion not supported on PAX tables");
}

static void
pax_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                        CommandId cid, int options, BulkInsertState bistate) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static TM_Result
pax_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
                  Snapshot snapshot, Snapshot crosscheck, bool wait,
                  TM_FailureData *tmfd, bool changingPart) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


static TM_Result
pax_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
                  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                  bool wait, TM_FailureData *tmfd,
                  LockTupleMode *lockmode, bool *update_indexes) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static TM_Result
pax_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
                      TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
                      LockWaitPolicy wait_policy, uint8 flags,
                      TM_FailureData *tmfd) {
    /* GPDB_12_MERGE_FIXME: not supported. Can this function be left out completely? Or ereport()? */
    elog(ERROR, "speculative insertion not supported on PAX tables");
}

static void
pax_finish_bulk_insert(Relation relation, int options) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for heap AM
 * ------------------------------------------------------------------------
 */

static bool
pax_fetch_row_version(Relation relation,
                             ItemPointer tid,
                             Snapshot snapshot,
                             TupleTableSlot *slot) {
    /*
     * This is a generic interface. It is currently used in three distinct
     * cases, only one of which is currently invoking it for AO tables.
     * This is DELETE RETURNING. In order to return the slot via the tid for
     * AO tables one would have to scan the block directory and the visibility
     * map. A block directory is not guarranteed to exist. Even if it exists, a
     * state would have to be created and dropped for every tuple look up since
     * this interface does not allow for the state to be passed around. This is
     * a very costly operation to be performed per tuple lookup. Furthermore, if
     * a DELETE operation is currently on the fly, the corresponding visibility
     * map entries will not have been finalized into a visibility map tuple.
     *
     * Error out with feature not supported. Given that this is a generic
     * interface, we can not really say which feature is that, although we do
     * know that is DELETE RETURNING.
     */
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("feature not supported on pax relations")));
}

static void
pax_get_latest_tid(TableScanDesc sscan,
                          ItemPointer tid) {
    /*
     * Tid scans are not supported for appendoptimized relation. This function
     * should not have been called in the first place, but if it is called,
     * better to error out.
     */
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("feature not supported on pax relations")));
}

static bool
pax_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
    /*
     * Tid scans are not supported for appendoptimized relation. This function
     * should not have been called in the first place, but if it is called,
     * better to error out.
     */
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("feature not supported on pax relations")));
}

static bool
pax_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                    Snapshot snapshot) {
    /*
     * AO_COLUMN table dose not support unique and tidscan yet.
     */
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("feature not supported on pax relations")));
}

static TransactionId
pax_index_delete_tuples(Relation rel,
                         TM_IndexDeleteOp *delstate) {
    // GPDB_14_MERGE_FIXME: vacuum related call back.
    elog(ERROR, "not implemented yet, %s", __func__);
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for ao_column AM.
 * ------------------------------------------------------------------------
 */

static void
pax_relation_set_new_filenode(Relation rel,
                               const RelFileNode *newrnode,
                               char persistence,
                               TransactionId *freezeXid,
                               MultiXactId *minmulti) {
    *freezeXid = *minmulti = InvalidTransactionId;
    elog(LOG, "pax_relation_set_new_filenode:%d/%d/%d", newrnode->dbNode, newrnode->spcNode, newrnode->relNode);
}


static void
pax_relation_nontransactional_truncate(Relation rel) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_relation_copy_data(Relation rel, const RelFileNode *newrnode) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
                                     Relation OldIndex, bool use_sort,
                                     TransactionId OldestXmin,
                                     TransactionId *xid_cutoff,
                                     MultiXactId *multi_cutoff,
                                     double *num_tuples,
                                     double *tups_vacuumed,
                                     double *tups_recently_dead) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


static void
pax_vacuum_rel(Relation onerel, VacuumParams *params,
                      BufferAccessStrategy bstrategy) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


static bool
pax_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                   BufferAccessStrategy bstrategy) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                   double *liverows, double *deadrows,
                                   TupleTableSlot *slot) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


static double
pax_index_build_range_scan(Relation heapRelation,
                                  Relation indexRelation,
                                  IndexInfo *indexInfo,
                                  bool allow_sync,
                                  bool anyvisible,
                                  bool progress,
                                  BlockNumber start_blockno,
                                  BlockNumber numblocks,
                                  IndexBuildCallback callback,
                                  void *callback_state,
                                  TableScanDesc scan) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static void
pax_index_validate_scan(Relation heapRelation,
                               Relation indexRelation,
                               IndexInfo *indexInfo,
                               Snapshot snapshot,
                               ValidateIndexState *state) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

/*
 * This pretends that the all the space is taken by the main fork.
 * Returns the compressed size.
 */
static uint64
pax_relation_size(Relation rel, ForkNumber forkNumber) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_relation_needs_toast_table(Relation rel) {
    /*
     * AO_COLUMN never used the toasting, don't create the toast table from
     * Cloudberry 7
     */
    return false;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */
static void
pax_estimate_rel_size(Relation rel, int32 *attr_widths,
                             BlockNumber *pages, double *tuples,
                             double *allvisfrac) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */
static bool
pax_scan_bitmap_next_block(TableScanDesc scan,
                                  TBMIterateResult *tbmres) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_scan_bitmap_next_tuple(TableScanDesc scan,
                            TBMIterateResult *tbmres,
                            TupleTableSlot *slot) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate) {
    elog(ERROR, "not implemented yet, %s", __func__);
}

static bool
pax_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
                                  TupleTableSlot *slot) {
    elog(ERROR, "not implemented yet, %s", __func__);
}


/* ------------------------------------------------------------------------
 * Definition of the PAX table access method.
 * ------------------------------------------------------------------------
 */
static TableAmRoutine pax_column_methods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = pax_slot_callbacks,

    /*
     * GPDB: it is needed to extract the column information for
     * scans before calling beginscan. This can not happen in beginscan because
     * the needed information is not available at that time. It is the caller's
     * responsibility to choose to call pax_beginscan_extractcolumns or
     * pax_beginscan.
     */
    .scan_begin_extractcolumns = pax_beginscan_extractcolumns,

    /*
     * GPDB: Like above but for bitmap scans.
     */
    .scan_begin_extractcolumns_bm = pax_beginscan_extractcolumns_bm,

    .scan_begin = pax_beginscan,
    .scan_end = pax_endscan,
    .scan_rescan = pax_rescan,
    .scan_getnextslot = pax_getnextslot,

    .parallelscan_estimate = pax_parallelscan_estimate,
    .parallelscan_initialize = pax_parallelscan_initialize,
    .parallelscan_reinitialize = pax_parallelscan_reinitialize,

    .index_fetch_begin = pax_index_fetch_begin,
    .index_fetch_reset = pax_index_fetch_reset,
    .index_fetch_end = pax_index_fetch_end,
    .index_fetch_tuple = pax_index_fetch_tuple,

    .tuple_insert = pax_tuple_insert,
    .tuple_insert_speculative = pax_tuple_insert_speculative,
    .tuple_complete_speculative = pax_tuple_complete_speculative,
    .multi_insert = pax_multi_insert,
    .tuple_delete = pax_tuple_delete,
    .tuple_update = pax_tuple_update,
    .tuple_lock = pax_tuple_lock,
    .finish_bulk_insert = pax_finish_bulk_insert,

    .tuple_fetch_row_version = pax_fetch_row_version,
    .tuple_get_latest_tid = pax_get_latest_tid,
    .tuple_tid_valid = pax_tuple_tid_valid,
    .tuple_satisfies_snapshot = pax_tuple_satisfies_snapshot,
    .index_delete_tuples = pax_index_delete_tuples,

    .relation_set_new_filenode = pax_relation_set_new_filenode,
    .relation_nontransactional_truncate = pax_relation_nontransactional_truncate,
    .relation_copy_data = pax_relation_copy_data,
    .relation_copy_for_cluster = pax_relation_copy_for_cluster,
    .relation_vacuum = pax_vacuum_rel,
    .scan_analyze_next_block = pax_scan_analyze_next_block,
    .scan_analyze_next_tuple = pax_scan_analyze_next_tuple,
    .index_build_range_scan = pax_index_build_range_scan,
    .index_validate_scan = pax_index_validate_scan,

    .relation_size = pax_relation_size,
    .relation_needs_toast_table = pax_relation_needs_toast_table,

    .relation_estimate_size = pax_estimate_rel_size,

    .scan_bitmap_next_block = pax_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = pax_scan_bitmap_next_tuple,
    .scan_sample_next_block = pax_scan_sample_next_block,
    .scan_sample_next_tuple = pax_scan_sample_next_tuple
};

Datum
pax_tableam_handler(PG_FUNCTION_ARGS) {
    PG_RETURN_POINTER(&pax_column_methods);
}
