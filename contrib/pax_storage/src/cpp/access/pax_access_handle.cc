#include "access/pax_access_handle.h"

#include "access/pax_access.h"
#include "access/pax_dml_state.h"
#include "access/pax_scanner.h"
#include "catalog/pax_aux_table.h"

#define NOT_IMPLEMENTED_YET                        \
  ereport(ERROR,                                   \
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
           errmsg("not implemented yet on pax relations: %s", __func__)))

#define NOT_SUPPORTED_YET                                 \
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
                  errmsg("not supported on pax relations: %s", __func__)))

#define PAX_DEFAULT_COMPRESSLEVEL AO_DEFAULT_COMPRESSLEVEL
#define PAX_MIN_COMPRESSLEVEL AO_MIN_COMPRESSLEVEL
#define PAX_MAX_COMPRESSLEVEL AO_MAX_COMPRESSLEVEL

#define PAX_DEFAULT_COMPRESSTYPE AO_DEFAULT_COMPRESSTYPE

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

// reloptions structure and variables.
static relopt_kind self_relopt_kind;
static const relopt_parse_elt self_relopt_tab[] = {
    {"compresslevel", RELOPT_TYPE_INT, offsetof(PaxOptions, compresslevel)},
    {"compresstype", RELOPT_TYPE_STRING, offsetof(PaxOptions, compresstype)},
    {"storage_format", RELOPT_TYPE_STRING,
     offsetof(PaxOptions, storage_format)},
};

// access methods that are implemented in C++
namespace pax {

TableScanDesc CCPaxAccessMethod::ScanBegin(Relation relation, Snapshot snapshot,
                                           int nkeys, struct ScanKeyData *key,
                                           ParallelTableScanDesc pscan,
                                           uint32 flags) {
  // todo: PAX_TRY {} PAX_CATCH {}
  return pax::CPaxAccess::PaxBeginScan(relation, snapshot, nkeys, key, pscan,
                                       flags);
}

void CCPaxAccessMethod::ScanEnd(TableScanDesc scan) {
  // todo PAX_TRY{} PAX_CATCH {}
  pax::CPaxAccess::PaxEndScan(scan);
}

void CCPaxAccessMethod::ScanRescan(TableScanDesc scan, ScanKey key,
                                   bool set_params, bool allow_strat,
                                   bool allow_sync, bool allow_pagemode) {
  NOT_IMPLEMENTED_YET;
}

bool CCPaxAccessMethod::ScanGetnextslot(TableScanDesc scan,
                                        ScanDirection direction,
                                        TupleTableSlot *slot) {
  return pax::CPaxAccess::PaxGetNextSlot(scan, direction, slot);
}

void CCPaxAccessMethod::TupleInsert(Relation relation, TupleTableSlot *slot,
                                    CommandId cid, int options,
                                    BulkInsertState bistate) {
  // todo C CALL C++ WRAPPER
  pax::CPaxAccess::PaxTupleInsert(relation, slot, cid, options, bistate);
}

TM_Result CCPaxAccessMethod::TupleDelete(Relation relation, ItemPointer tid,
                                         CommandId cid, Snapshot snapshot,
                                         Snapshot crosscheck, bool wait,
                                         TM_FailureData *tmfd,
                                         bool changingPart) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
}

TM_Result CCPaxAccessMethod::TupleUpdate(Relation relation, ItemPointer otid,
                                         TupleTableSlot *slot, CommandId cid,
                                         Snapshot snapshot, Snapshot crosscheck,
                                         bool wait, TM_FailureData *tmfd,
                                         LockTupleMode *lockmode,
                                         bool *update_indexes) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
}

bool CCPaxAccessMethod::ScanAnalyzeNextBlock(TableScanDesc scan,
                                             BlockNumber blockno,
                                             BufferAccessStrategy bstrategy) {
  PaxScanDesc *paxscan = reinterpret_cast<PaxScanDesc *>(scan);
  paxscan->targetTupleId = blockno;

  return true;
}

bool CCPaxAccessMethod::ScanAnalyzeNextTuple(TableScanDesc scan,
                                             TransactionId OldestXmin,
                                             double *liverows, double *deadrows,
                                             TupleTableSlot *slot) {
  PaxScanDesc *paxscan = reinterpret_cast<PaxScanDesc *>(scan);
  bool ret = false;

  Assert(*deadrows == 0);  // not dead rows in pax latest snapshot

  // skip several tuples if they are not sampling target.
  ret = paxscan->scanner->SeekTuple(paxscan->targetTupleId,
                                    &(paxscan->nextTupleId));

  if (!ret) {
    return false;
  }

  ret = ScanGetnextslot(scan, ForwardScanDirection, slot);
  paxscan->nextTupleId++;
  if (ret) {
    *liverows += 1;
  }

  // Unlike heap table, latest pax snapshot does not contain deadrows,
  // so `false` value of ret indicate that no more tuple to fetch,
  // and just return directly.
  return ret;
}

bool CCPaxAccessMethod::ScanBitmapNextBlock(TableScanDesc scan,
                                            TBMIterateResult *tbmres) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanBitmapNextTuple(TableScanDesc scan,
                                            TBMIterateResult *tbmres,
                                            TupleTableSlot *slot) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanSampleNextBlock(TableScanDesc scan,
                                            SampleScanState *scanstate) {
  PaxScanDesc *paxscan = reinterpret_cast<PaxScanDesc *>(scan);
  TsmRoutine *tsm = scanstate->tsmroutine;
  BlockNumber blockno = 0;
  BlockNumber pages = 0;
  double totaltuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;

  if (paxscan->totalTuples == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(scan->rs_rd, &attrwidths, &pages,
                                           &totaltuples, &allvisfrac);
    paxscan->totalTuples = totaltuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, paxscan->totalTuples);
  else
    blockno = system_nextsampleblock(scanstate, paxscan->totalTuples);

  if (!BlockNumberIsValid(blockno)) return false;

  paxscan->fetchTupleId = blockno;
  return true;
}

bool CCPaxAccessMethod::ScanSampleNextTuple(TableScanDesc scan,
                                            SampleScanState *scanstate,
                                            TupleTableSlot *slot) {
  PaxScanDesc *paxscan = reinterpret_cast<PaxScanDesc *>(scan);
  bool ret = false;

  // skip several tuples if they are not sampling target.
  ret = paxscan->scanner->SeekTuple(paxscan->fetchTupleId,
                                    &(paxscan->nextTupleId));

  if (!ret) return false;

  ret = ScanGetnextslot(scan, ForwardScanDirection, slot);
  paxscan->nextTupleId++;

  return ret;
}

void CCPaxAccessMethod::MultiInsert(Relation relation, TupleTableSlot **slots,
                                  int ntuples, CommandId cid, int options,
                                  BulkInsertState bistate) {
  CPaxInserter *inserter = pax::CPaxDmlStateLocal::instance()->GetInserter(relation);
  Assert(inserter != nullptr);
  // TODO(Tony): implement bulk insert as AO/HEAP does with tuples iteration, which
  // needs to be futher optmized by using new feature like Parallelization or Vectorization.
  for (int i = 0; i < ntuples; i++) {
    inserter->InsertTuple(relation, slots[i], cid, options, bistate);
  }
}

void CCPaxAccessMethod::FinishBulkInsert(Relation relation, int options) {
  // Implement Pax dml cleanup for case "create table xxx1 as select * from xxx2",
  // which would not call ExtDmlFini callback function and relies on FinishBulkInsert
  // callback function to cleanup its dml state.
  pax::CPaxDmlStateLocal::instance()->FinishDmlState(relation, CMD_INSERT);
}

void CCPaxAccessMethod::ExtDmlInit(Relation rel, CmdType operation) {
  if (RelationIsPax(rel))
    pax::CPaxDmlStateLocal::instance()->InitDmlState(rel, operation);
}

void CCPaxAccessMethod::ExtDmlFini(Relation rel, CmdType operation) {
  if (RelationIsPax(rel))
    pax::CPaxDmlStateLocal::instance()->FinishDmlState(rel, operation);
}

}  // namespace pax
// END of C++ implementation

// access methods that are implemented in C
namespace paxc {
const TupleTableSlotOps *PaxAccessMethod::SlotCallbacks(Relation rel) noexcept {
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

TM_Result PaxAccessMethod::TupleLock(Relation relation, ItemPointer tid,
                                     Snapshot snapshot, TupleTableSlot *slot,
                                     CommandId cid, LockTupleMode mode,
                                     LockWaitPolicy wait_policy, uint8 flags,
                                     TM_FailureData *tmfd) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
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

  elog(DEBUG1, "pax_relation_set_new_filenode:%d/%d/%d", newrnode->dbNode,
       newrnode->spcNode, newrnode->relNode);

  *freezeXid = *minmulti = InvalidTransactionId;
  tupcache = SearchSysCache1(PAXTABLESID, RelationGetRelid(rel));
  if (HeapTupleIsValid(tupcache)) {
    Relation auxRel;
    Oid auxRelid;
    auxRelid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;
    ReleaseSysCache(tupcache);

    Assert(OidIsValid(auxRelid));
    // truncate already exist pax block auxiliary table.
    auxRel = relation_open(auxRelid, AccessExclusiveLock);

    /*TODO1 pending-delete operation should be applied here. */
    RelationSetNewRelfilenode(auxRel, auxRel->rd_rel->relpersistence);
    relation_close(auxRel, NoLock);
  } else {
    // create pg_pax_blocks_<pax_table_oid>
    pax::CPaxAccess::PaxCreateAuxBlocks(rel);
  }
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
void PaxAccessMethod::RelationNontransactionalTruncate(Relation rel) {
  HeapTuple tupcache;
  Relation auxRel;
  Oid auxRelid;

  elog(DEBUG1, "pax_relation_nontransactional_truncate:%u/%u/%u",
       rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode);

  tupcache = SearchSysCache1(PAXTABLESID, RelationGetRelid(rel));
  if (!HeapTupleIsValid(tupcache))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cache lookup failed with relid=%u for aux relation "
                           "in pg_pax_tables.",
                           RelationGetRelid(rel))));
  auxRelid = ((Form_pg_pax_tables)GETSTRUCT(tupcache))->blocksrelid;
  ReleaseSysCache(tupcache);

  Assert(OidIsValid(auxRelid));
  auxRel = relation_open(auxRelid, AccessExclusiveLock);
  heap_truncate_one_rel(auxRel);
  relation_close(auxRel, NoLock);
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

  if (forkNumber != MAIN_FORKNUM) return 0;

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

#define PAX_COPY_OPT(pax_opts_, pax_opt_name_)                                \
  do {                                                                        \
    PaxOptions *pax_opts = reinterpret_cast<PaxOptions *>(pax_opts_);         \
    int pax_name_offset_ = *reinterpret_cast<int *>(pax_opts->pax_opt_name_); \
    if (pax_name_offset_)                                                     \
      strlcpy(pax_opts->pax_opt_name_,                                        \
              reinterpret_cast<char *>(pax_opts) + pax_name_offset_,          \
              sizeof(pax_opts->pax_opt_name_));                               \
  } while (0)
bytea *PaxAccessMethod::Amoptions(Datum reloptions, char relkind,
                                  bool validate) {
  void *rdopts;
  rdopts = build_reloptions(reloptions, validate, self_relopt_kind,
                            sizeof(PaxOptions), self_relopt_tab,
                            lengthof(self_relopt_tab));
  // adjust string values
  PAX_COPY_OPT(rdopts, storage_format);
  PAX_COPY_OPT(rdopts, compresstype);

  return reinterpret_cast<bytea *>(rdopts);
}
#undef PAX_COPY_OPT

}  // namespace paxc
// END of C implementation

extern "C" {

static const TableAmRoutine pax_column_methods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = paxc::PaxAccessMethod::SlotCallbacks,
    .scan_begin = pax::CCPaxAccessMethod::ScanBegin,
    .scan_end = pax::CCPaxAccessMethod::ScanEnd,
    .scan_rescan = pax::CCPaxAccessMethod::ScanRescan,
    .scan_getnextslot = pax::CCPaxAccessMethod::ScanGetnextslot,

    .parallelscan_estimate = paxc::PaxAccessMethod::ParallelscanEstimate,
    .parallelscan_initialize = paxc::PaxAccessMethod::ParallelscanInitialize,
    .parallelscan_reinitialize =
        paxc::PaxAccessMethod::ParallelscanReinitialize,

    .index_fetch_begin = paxc::PaxAccessMethod::IndexFetchBegin,
    .index_fetch_reset = paxc::PaxAccessMethod::IndexFetchReset,
    .index_fetch_end = paxc::PaxAccessMethod::IndexFetchEnd,
    .index_fetch_tuple = paxc::PaxAccessMethod::IndexFetchTuple,

    .tuple_fetch_row_version = paxc::PaxAccessMethod::TupleFetchRowVersion,
    .tuple_tid_valid = paxc::PaxAccessMethod::TupleTidValid,
    .tuple_get_latest_tid = paxc::PaxAccessMethod::TupleGetLatestTid,
    .tuple_satisfies_snapshot = paxc::PaxAccessMethod::TupleSatisfiesSnapshot,
    .index_delete_tuples = paxc::PaxAccessMethod::IndexDeleteTuples,

    .tuple_insert = pax::CCPaxAccessMethod::TupleInsert,
    .tuple_insert_speculative = paxc::PaxAccessMethod::TupleInsertSpeculative,
    .tuple_complete_speculative =
        paxc::PaxAccessMethod::TupleCompleteSpeculative,
    .multi_insert = pax::CCPaxAccessMethod::MultiInsert,
    .tuple_delete = pax::CCPaxAccessMethod::TupleDelete,
    .tuple_update = pax::CCPaxAccessMethod::TupleUpdate,
    .tuple_lock = paxc::PaxAccessMethod::TupleLock,
    .finish_bulk_insert = pax::CCPaxAccessMethod::FinishBulkInsert,

    .relation_set_new_filenode = paxc::PaxAccessMethod::RelationSetNewFilenode,
    .relation_nontransactional_truncate =
        paxc::PaxAccessMethod::RelationNontransactionalTruncate,
    .relation_copy_data = paxc::PaxAccessMethod::RelationCopyData,
    .relation_copy_for_cluster = paxc::PaxAccessMethod::RelationCopyForCluster,
    .relation_vacuum = paxc::PaxAccessMethod::RelationVacuum,
    .scan_analyze_next_block = pax::CCPaxAccessMethod::ScanAnalyzeNextBlock,
    .scan_analyze_next_tuple = pax::CCPaxAccessMethod::ScanAnalyzeNextTuple,
    .index_build_range_scan = paxc::PaxAccessMethod::IndexBuildRangeScan,
    .index_validate_scan = paxc::PaxAccessMethod::IndexValidateScan,

    .relation_size = paxc::PaxAccessMethod::RelationSize,
    .relation_needs_toast_table =
        paxc::PaxAccessMethod::RelationNeedsToastTable,

    .relation_estimate_size = paxc::PaxAccessMethod::EstimateRelSize,
    .scan_bitmap_next_block = pax::CCPaxAccessMethod::ScanBitmapNextBlock,
    .scan_bitmap_next_tuple = pax::CCPaxAccessMethod::ScanBitmapNextTuple,
    .scan_sample_next_block = pax::CCPaxAccessMethod::ScanSampleNextBlock,
    .scan_sample_next_tuple = pax::CCPaxAccessMethod::ScanSampleNextTuple,

    .amoptions = paxc::PaxAccessMethod::Amoptions,
};

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);
Datum pax_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&pax_column_methods);
}

static void pax_validate_storage_format(const char *value) {
  size_t i;
  static const char *storage_formats[] = {
      "orc",
      "ppt",
  };

  for (i = 0; i < lengthof(storage_formats); i++) {
    if (strcmp(value, storage_formats[i]) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported storage format: '%s'", value)));
}

static void pax_validate_compresstype(const char *value) {
  size_t i;
  static const char *compress_types[] = {
      "none",
      "zlib",
  };

  for (i = 0; i < lengthof(compress_types); i++) {
    if (strcmp(value, compress_types[i]) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported compress type: '%s'", value)));
}

void _PG_init(void) {
  ext_dml_init_hook = pax::CCPaxAccessMethod::ExtDmlInit;
  ext_dml_finish_hook = pax::CCPaxAccessMethod::ExtDmlFini;

  self_relopt_kind = add_reloption_kind();
  add_string_reloption(self_relopt_kind, "storage_format", "pax storage format",
                       "orc", pax_validate_storage_format, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, "compresstype", "pax compress type",
                       PAX_DEFAULT_COMPRESSTYPE, pax_validate_compresstype,
                       AccessExclusiveLock);
  add_int_reloption(self_relopt_kind, "compresslevel", "pax compress level",
                    PAX_DEFAULT_COMPRESSLEVEL, AO_MIN_COMPRESSLEVEL,
                    AO_MAX_COMPRESSLEVEL, AccessExclusiveLock);
}

}  // extern "C"
