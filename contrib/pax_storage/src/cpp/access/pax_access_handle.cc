#include "access/pax_access_handle.h"

#include "access/pax_access.h"
#include "access/pax_dml_state.h"
#include "catalog/pax_aux_table.h"

extern "C" {
#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h"  // NOLINT
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

#define RelationIsPax(rel) AMOidIsPax((rel)->rd_rel->relam)

bool AMOidIsPax(Oid amOid) {
  HeapTuple tuple;
  Form_pg_am form;
  bool is_pax;

  tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for pg_am.oid = %u", amOid);

  form = (Form_pg_am)GETSTRUCT(tuple);
  is_pax = strcmp(NameStr(form->amname), "pax") == 0;
  ReleaseSysCache(tuple);

  return is_pax;
}

namespace pax {
const TupleTableSlotOps *PaxAccessMethod::SlotCallbacks(Relation rel) {
  return &TTSOpsVirtual;
}

Size PaxAccessMethod::ParallelscanEstimate(Relation rel) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

Size PaxAccessMethod::ParallelscanInitialize(Relation rel,
                                             ParallelTableScanDesc pscan) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

void PaxAccessMethod::ParallelscanReinitialize(Relation rel,
                                               ParallelTableScanDesc pscan) {
  NOT_IMPLEMENTED_YET;
}

struct IndexFetchTableData *PaxAccessMethod::IndexFetchBegin(Relation rel) {
  NOT_SUPPORTED_YET;
  return nullptr;
}

void PaxAccessMethod::IndexFetchEnd(IndexFetchTableData *scan) {
  NOT_SUPPORTED_YET;
}

void PaxAccessMethod::IndexFetchReset(IndexFetchTableData *scan) {
  NOT_SUPPORTED_YET;
}

bool PaxAccessMethod::IndexFetchTuple(struct IndexFetchTableData *scan,
                                      ItemPointer tid, Snapshot snapshot,
                                      TupleTableSlot *slot, bool *call_again,
                                      bool *all_dead) {
  NOT_SUPPORTED_YET;
  return false;
}

void PaxAccessMethod::TupleInsertSpeculative(Relation relation,
                                             TupleTableSlot *slot,
                                             CommandId cid, int options,
                                             BulkInsertState bistate,
                                             uint32 specToken) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::TupleCompleteSpeculative(Relation relation,
                                               TupleTableSlot *slot,
                                               uint32 specToken,
                                               bool succeeded) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::MultiInsert(Relation relation, TupleTableSlot **slots,
                                  int ntuples, CommandId cid, int options,
                                  BulkInsertState bistate) {
  NOT_IMPLEMENTED_YET;
}

TM_Result PaxAccessMethod::TupleLock(Relation relation, ItemPointer tid,
                                     Snapshot snapshot, TupleTableSlot *slot,
                                     CommandId cid, LockTupleMode mode,
                                     LockWaitPolicy wait_policy, uint8 flags,
                                     TM_FailureData *tmfd) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
}

void PaxAccessMethod::FinishBulkInsert(Relation relation, int options) {
  NOT_IMPLEMENTED_YET;
}

bool PaxAccessMethod::TupleFetchRowVersion(Relation relation, ItemPointer tid,
                                           Snapshot snapshot,
                                           TupleTableSlot *slot) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool PaxAccessMethod::TupleTidValid(TableScanDesc scan, ItemPointer tid) {
  NOT_IMPLEMENTED_YET;
  return false;
}

void PaxAccessMethod::TupleGetLatestTid(TableScanDesc sscan, ItemPointer tid) {
  NOT_SUPPORTED_YET;
}

bool PaxAccessMethod::TupleSatisfiesSnapshot(Relation rel, TupleTableSlot *slot,
                                             Snapshot snapshot) {
  NOT_IMPLEMENTED_YET;
  return true;
}

TransactionId PaxAccessMethod::IndexDeleteTuples(Relation rel,
                                                 TM_IndexDeleteOp *delstate) {
  NOT_SUPPORTED_YET;
  return 0;
}

void PaxAccessMethod::RelationSetNewFilenode(Relation rel,
                                             const RelFileNode *newrnode,
                                             char persistence,
                                             TransactionId *freezeXid,
                                             MultiXactId *minmulti) {
  HeapTuple tupcache;
  Oid blocksrelid;
  *freezeXid = *minmulti = InvalidTransactionId;

  elog(DEBUG1, "pax_relation_set_new_filenode:%d/%d/%d", newrnode->dbNode,
       newrnode->spcNode, newrnode->relNode);

  // create pg_pax_blocks_<pax_table_oid>
  tupcache = SearchSysCache1(PAXTABLESID, RelationGetRelid(rel));
  if (HeapTupleIsValid(tupcache)) {
    blocksrelid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;
    ReleaseSysCache(tupcache);
    pax::CPaxAccess::PaxTransactionalTruncateTable(blocksrelid);
  } else {
    pax::CPaxAccess::PaxCreateAuxBlocks(rel);
  }
}

// * non transactional truncate table case:
// create table inside transactional block, and then truncate table inside
// transactional block. create table outside transactional block, insert data
// and truncate table inside transactional block.
void PaxAccessMethod::RelationNontransactionalTruncate(Relation rel) {
  HeapTuple tupcache;
  Oid blocksrelid;

  elog(DEBUG1, "pax_relation_nontransactional_truncate:%d/%d/%d",
       rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode);

  tupcache = SearchSysCache1(PAXTABLESID, RelationGetRelid(rel));
  if (!HeapTupleIsValid(tupcache))
    ereport(
        ERROR,
        (errcode(ERRCODE_UNDEFINED_SCHEMA),
         errmsg(
             "relid with %d find no oid of the aux relation in pg_pax_tables.",
             rel->rd_id)));
  blocksrelid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;
  ReleaseSysCache(tupcache);
  pax::CPaxAccess::PaxNonTransactionalTruncateTable(blocksrelid);
}

void PaxAccessMethod::RelationCopyData(Relation rel,
                                       const RelFileNode *newrnode) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::RelationCopyForCluster(
    Relation OldHeap, Relation NewHeap, Relation OldIndex, bool use_sort,
    TransactionId OldestXmin, TransactionId *xid_cutoff,
    MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed,
    double *tups_recently_dead) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::RelationVacuum(Relation onerel, VacuumParams *params,
                                     BufferAccessStrategy bstrategy) {
  NOT_IMPLEMENTED_YET;
}

uint64 PaxAccessMethod::RelationSize(Relation rel, ForkNumber forkNumber) {
  Oid paxAuxOid;
  Relation paxAuxRel;
  TupleDesc auxTupDesc;
  HeapTuple auxTup;
  SysScanDesc auxScan;
  uint64 paxSize = 0;

  if (forkNumber != MAIN_FORKNUM)
    return 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  GetPaxTablesEntryAttributes(rel->rd_id, &paxAuxOid, NULL, NULL);

  // Scan pg_pax_blocks_xxx to calculate size of micro partition
  paxAuxRel = heap_open(paxAuxOid, AccessShareLock);
  auxTupDesc = RelationGetDescr(paxAuxRel);

  auxScan = systable_beginscan(paxAuxRel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(auxTup = systable_getnext(auxScan))) {
    bool isnull = false;
    // TODO(chenhongjie): Exactly what is needed and being obtained is
    // compressed size. Later, when the aux table supports size attributes
    // before/after compression, we need to distinguish two attributes by names.
    Datum tupDatum = heap_getattr(auxTup, Anum_pg_pax_block_tables_ptblocksize,
                                  auxTupDesc, &isnull);

    Assert(!isnull);
    paxSize += DatumGetUInt32(tupDatum);
  }

  systable_endscan(auxScan);
  heap_close(paxAuxRel, AccessShareLock);

  return paxSize;
}

bool PaxAccessMethod::RelationNeedsToastTable(Relation rel) {
  // PAX never used the toasting, don't create the toast table from Cloudberry 7
  return false;
}

// Similar to the case of AO and AOCS tables, PAX table has auxiliary tables,
// size can be read directly from the auxiliary table, and there is not much
// space for optimization in estimating relsize. So this function is implemented
// in the same way as pax_relation_size().
void PaxAccessMethod::EstimateRelSize(Relation rel, int32 *attr_widths,
                                      BlockNumber *pages, double *tuples,
                                      double *allvisfrac) {
  Oid paxAuxOid;
  Relation paxAuxRel;
  TupleDesc auxTupDesc;
  HeapTuple auxTup;
  SysScanDesc auxScan;
  uint32 totalTuples = 0;
  uint64 paxSize = 0;

  // Even an empty table takes at least one page,
  // but number of tuples for an empty table could be 0.
  *tuples = 0;
  *pages = 1;
  // index-only scan is not supported in PAX
  *allvisfrac = 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  GetPaxTablesEntryAttributes(rel->rd_id, &paxAuxOid, NULL, NULL);

  // Scan pg_pax_blocks_xxx to get attributes
  paxAuxRel = heap_open(paxAuxOid, AccessShareLock);
  auxTupDesc = RelationGetDescr(paxAuxRel);

  auxScan = systable_beginscan(paxAuxRel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(auxTup = systable_getnext(auxScan))) {
    Datum pttupcountDatum;
    Datum ptblocksizeDatum;
    bool isnull = false;

    pttupcountDatum = heap_getattr(auxTup, Anum_pg_pax_block_tables_pttupcount,
                                   auxTupDesc, &isnull);
    Assert(!isnull);
    totalTuples += DatumGetUInt32(pttupcountDatum);

    isnull = false;
    // TODO(chenhongjie): Exactly what we want to get here is uncompressed size,
    // but what we're getting is compressed size. Later, when the aux table
    // supports size attributes before/after compression, this needs to
    // be corrected.
    ptblocksizeDatum = heap_getattr(
        auxTup, Anum_pg_pax_block_tables_ptblocksize, auxTupDesc, &isnull);

    Assert(!isnull);
    paxSize += DatumGetUInt32(ptblocksizeDatum);
  }

  systable_endscan(auxScan);
  heap_close(paxAuxRel, AccessShareLock);

  *tuples = static_cast<double>(totalTuples);
  *pages = RelationGuessNumberOfBlocksFromSize(paxSize);

  return;
}

double PaxAccessMethod::IndexBuildRangeScan(
    Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
    BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
    TableScanDesc scan) {
  NOT_SUPPORTED_YET;
  return 0.0;
}

void PaxAccessMethod::IndexValidateScan(Relation heapRelation,
                                        Relation indexRelation,
                                        IndexInfo *indexInfo, Snapshot snapshot,
                                        ValidateIndexState *state) {
  NOT_IMPLEMENTED_YET;
}
}  // namespace pax

static void pax_dml_init(Relation rel, CmdType operation) {
  if (RelationIsPax(rel))
    pax::CPaxDmlStateLocal::instance()->InitDmlState(rel, operation);
}

static void pax_dml_fini(Relation rel, CmdType operation) {
  if (RelationIsPax(rel))
    pax::CPaxDmlStateLocal::instance()->FinishDmlState(rel, operation);
}

extern "C" {

static TableAmRoutine pax_column_methods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = pax::PaxAccessMethod::SlotCallbacks,
    .scan_begin = pax::CCPaxAccessMethod::ScanBegin,
    .scan_end = pax::CCPaxAccessMethod::ScanEnd,
    .scan_rescan = pax::CCPaxAccessMethod::ScanRescan,
    .scan_getnextslot = pax::CCPaxAccessMethod::ScanGetnextslot,

    .parallelscan_estimate = pax::PaxAccessMethod::ParallelscanEstimate,
    .parallelscan_initialize = pax::PaxAccessMethod::ParallelscanInitialize,
    .parallelscan_reinitialize = pax::PaxAccessMethod::ParallelscanReinitialize,

    .index_fetch_begin = pax::PaxAccessMethod::IndexFetchBegin,
    .index_fetch_reset = pax::PaxAccessMethod::IndexFetchReset,
    .index_fetch_end = pax::PaxAccessMethod::IndexFetchEnd,
    .index_fetch_tuple = pax::PaxAccessMethod::IndexFetchTuple,

    .tuple_fetch_row_version = pax::PaxAccessMethod::TupleFetchRowVersion,
    .tuple_tid_valid = pax::PaxAccessMethod::TupleTidValid,
    .tuple_get_latest_tid = pax::PaxAccessMethod::TupleGetLatestTid,
    .tuple_satisfies_snapshot = pax::PaxAccessMethod::TupleSatisfiesSnapshot,
    .index_delete_tuples = pax::PaxAccessMethod::IndexDeleteTuples,

    .tuple_insert = pax::CCPaxAccessMethod::TupleInsert,
    .tuple_insert_speculative = pax::PaxAccessMethod::TupleInsertSpeculative,
    .tuple_complete_speculative =
        pax::PaxAccessMethod::TupleCompleteSpeculative,
    .multi_insert = pax::PaxAccessMethod::MultiInsert,
    .tuple_delete = pax::CCPaxAccessMethod::TupleDelete,
    .tuple_update = pax::CCPaxAccessMethod::TupleUpdate,
    .tuple_lock = pax::PaxAccessMethod::TupleLock,
    .finish_bulk_insert = pax::PaxAccessMethod::FinishBulkInsert,

    .relation_set_new_filenode = pax::PaxAccessMethod::RelationSetNewFilenode,
    .relation_nontransactional_truncate =
        pax::PaxAccessMethod::RelationNontransactionalTruncate,
    .relation_copy_data = pax::PaxAccessMethod::RelationCopyData,
    .relation_copy_for_cluster = pax::PaxAccessMethod::RelationCopyForCluster,
    .relation_vacuum = pax::PaxAccessMethod::RelationVacuum,
    .scan_analyze_next_block = pax::CCPaxAccessMethod::ScanAnalyzeNextBlock,
    .scan_analyze_next_tuple = pax::CCPaxAccessMethod::ScanAnalyzeNextTuple,
    .index_build_range_scan = pax::PaxAccessMethod::IndexBuildRangeScan,
    .index_validate_scan = pax::PaxAccessMethod::IndexValidateScan,

    .relation_size = pax::PaxAccessMethod::RelationSize,
    .relation_needs_toast_table = pax::PaxAccessMethod::RelationNeedsToastTable,

    .relation_estimate_size = pax::PaxAccessMethod::EstimateRelSize,
    .scan_bitmap_next_block = pax::CCPaxAccessMethod::ScanBitmapNextBlock,
    .scan_bitmap_next_tuple = pax::CCPaxAccessMethod::ScanBitmapNextTuple,
    .scan_sample_next_block = pax::CCPaxAccessMethod::ScanSampleNextBlock,
    .scan_sample_next_tuple = pax::CCPaxAccessMethod::ScanSampleNextTuple,
};

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);
Datum pax_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&pax_column_methods);
}

void _PG_init(void) {
  ext_dml_init_hook = pax_dml_init;
  ext_dml_finish_hook = pax_dml_fini;
}

}  // extern "C"
