#include "access/pax_access_handle.h"

#include "access/pax_access.h"
#include "access/pax_dml_state.h"

extern "C" {
#include "access/heapam.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "cdb/cdbcustomam.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h"  // NOLINT
#include "utils/memutils.h"
#include "utils/rel.h"
}

extern "C" {

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);

void pax_dml_state_reset_cb(void *_) {
  pax::CPaxDmlStateLocal::instance()->reset();
}

TableScanDesc pax_beginscan(Relation relation, Snapshot snapshot, int nkeys,
                            struct ScanKeyData *key,
                            ParallelTableScanDesc pscan, uint32 flags) {
  // todo: PAX_TRY {} PAX_CATCH {}
  return pax::CPaxAccess::PaxBeginScan(relation, snapshot, nkeys, key, pscan,
                                       flags);
}

void pax_endscan(TableScanDesc scan) {
  // todo PAX_TRY{} PAX_CATCH {}
  pax::CPaxAccess::PaxEndScan(scan);
}

bool pax_getnextslot(TableScanDesc scan, ScanDirection direction,
                     TupleTableSlot *slot) {
  return pax::CPaxAccess::PaxGetNextSlot(scan, direction, slot);
}

void pax_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                      int options, BulkInsertState bistate) {
  // todo C CALL C++ WRAPPER
  pax::CPaxAccess::PaxTupleInsert(relation, slot, cid, options, bistate);
}

const TupleTableSlotOps *pax_slot_callbacks(Relation rel) {
  // FIXME: copy from aoco table.  do we need to provide ourself tuple format?
  return &TTSOpsVirtual;
}

static void pax_rescan(TableScanDesc scan, ScanKey key, bool set_params,
                       bool allow_strat, bool allow_sync, bool allow_pagemode) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static Size pax_parallelscan_estimate(Relation rel) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static Size pax_parallelscan_initialize(Relation rel,
                                        ParallelTableScanDesc pscan) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_parallelscan_reinitialize(Relation rel,
                                          ParallelTableScanDesc pscan) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static IndexFetchTableData *pax_index_fetch_begin(Relation rel) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_index_fetch_reset(IndexFetchTableData *scan) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_index_fetch_end(IndexFetchTableData *scan) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_index_fetch_tuple(struct IndexFetchTableData *scan,
                                  ItemPointer tid, Snapshot snapshot,
                                  TupleTableSlot *slot, bool *call_again,
                                  bool *all_dead) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_tuple_insert_speculative(Relation relation,
                                         TupleTableSlot *slot, CommandId cid,
                                         int options, BulkInsertState bistate,
                                         uint32 specToken) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_tuple_complete_speculative(Relation relation,
                                           TupleTableSlot *slot,
                                           uint32 specToken, bool succeeded) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_multi_insert(Relation relation, TupleTableSlot **slots,
                             int ntuples, CommandId cid, int options,
                             BulkInsertState bistate) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static TM_Result pax_tuple_delete(Relation relation, ItemPointer tid,
                                  CommandId cid, Snapshot snapshot,
                                  Snapshot crosscheck, bool wait,
                                  TM_FailureData *tmfd, bool changingPart) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static TM_Result pax_tuple_update(Relation relation, ItemPointer otid,
                                  TupleTableSlot *slot, CommandId cid,
                                  Snapshot snapshot, Snapshot crosscheck,
                                  bool wait, TM_FailureData *tmfd,
                                  LockTupleMode *lockmode,
                                  bool *update_indexes) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static TM_Result pax_tuple_lock(Relation relation, ItemPointer tid,
                                Snapshot snapshot, TupleTableSlot *slot,
                                CommandId cid, LockTupleMode mode,
                                LockWaitPolicy wait_policy, uint8 flags,
                                TM_FailureData *tmfd) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_finish_bulk_insert(Relation relation, int options) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_fetch_row_version(Relation relation, ItemPointer tid,
                                  Snapshot snapshot, TupleTableSlot *slot) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_get_latest_tid(TableScanDesc sscan, ItemPointer tid) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                         Snapshot snapshot) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static TransactionId pax_index_delete_tuples(Relation rel,
                                             TM_IndexDeleteOp *delstate) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_relation_set_new_filenode(Relation rel,
                                          const RelFileNode *newrnode,
                                          char persistence,
                                          TransactionId *freezeXid,
                                          MultiXactId *minmulti) {
  *freezeXid = *minmulti = InvalidTransactionId;
  // todo: add c call c++ wrapper
  pax::CPaxAccess::PaxCreateAuxBlocks(rel, newrnode->relNode);
}

static void pax_relation_nontransactional_truncate(Relation rel) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_relation_copy_data(Relation rel, const RelFileNode *newrnode) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_relation_copy_for_cluster(
    Relation OldHeap, Relation NewHeap, Relation OldIndex, bool use_sort,
    TransactionId OldestXmin, TransactionId *xid_cutoff,
    MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed,
    double *tups_recently_dead) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_vacuum_rel(Relation onerel, VacuumParams *params,
                           BufferAccessStrategy bstrategy) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                        BufferAccessStrategy bstrategy) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_analyze_next_tuple(TableScanDesc scan,
                                        TransactionId OldestXmin,
                                        double *liverows, double *deadrows,
                                        TupleTableSlot *slot) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static double pax_index_build_range_scan(
    Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
    BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
    TableScanDesc scan) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_index_validate_scan(Relation heapRelation,
                                    Relation indexRelation,
                                    IndexInfo *indexInfo, Snapshot snapshot,
                                    ValidateIndexState *state) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static uint64 pax_relation_size(Relation rel, ForkNumber forkNumber) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_relation_needs_toast_table(Relation rel) {
  // PAX never used the toasting, don't create the toast table from Cloudberry 7
  return false;
}

static void pax_estimate_rel_size(Relation rel, int32 *attr_widths,
                                  BlockNumber *pages, double *tuples,
                                  double *allvisfrac) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_bitmap_next_block(TableScanDesc scan,
                                       TBMIterateResult *tbmres) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_bitmap_next_tuple(TableScanDesc scan,
                                       TBMIterateResult *tbmres,
                                       TupleTableSlot *slot) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_sample_next_block(TableScanDesc scan,
                                       SampleScanState *scanstate) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static bool pax_scan_sample_next_tuple(TableScanDesc scan,
                                       SampleScanState *scanstate,
                                       TupleTableSlot *slot) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("feature not supported on pax relations")));
}

static void pax_dml_init(Relation rel, CmdType operation) {
  // todo C CALL C++ WRAPPER
  pax::CPaxDmlStateLocal::instance()->InitDmlState(rel, operation);
  // todo
}

static void pax_dml_finish(Relation rel, CmdType operation) {
  pax::CPaxDmlStateLocal::instance()->FinishDmlState(rel, operation);
}

static TableAmRoutine pax_column_methods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = pax_slot_callbacks,
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

    .tuple_fetch_row_version = pax_fetch_row_version,
    .tuple_tid_valid = pax_tuple_tid_valid,
    .tuple_get_latest_tid = pax_get_latest_tid,
    .tuple_satisfies_snapshot = pax_tuple_satisfies_snapshot,
    .index_delete_tuples = pax_index_delete_tuples,

    .tuple_insert = pax_tuple_insert,
    .tuple_insert_speculative = pax_tuple_insert_speculative,
    .tuple_complete_speculative = pax_tuple_complete_speculative,
    .multi_insert = pax_multi_insert,
    .tuple_delete = pax_tuple_delete,
    .tuple_update = pax_tuple_update,
    .tuple_lock = pax_tuple_lock,
    .finish_bulk_insert = pax_finish_bulk_insert,

    .relation_set_new_filenode = pax_relation_set_new_filenode,
    .relation_nontransactional_truncate =
        pax_relation_nontransactional_truncate,
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
    .scan_sample_next_tuple = pax_scan_sample_next_tuple};

Datum pax_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&pax_column_methods);
}

void _PG_init(void) {
  // todo should add pre_dml_init_hook?
  pax_dml_init_hook = pax_dml_init;
  pax_dml_finish_hook = pax_dml_finish;
}
}
