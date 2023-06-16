#include "access/pax_access_handle.h"

#include "comm/cbdb_api.h"

#include "access/pax_dml_state.h"
#include "access/pax_scanner.h"
#include "access/pax_updater.h"
#include "catalog/pax_aux_table.h"
#include "comm/paxc_utils.h"
#include "exceptions/CException.h"
#include "storage/paxc_block_map_manager.h"

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

// CBDB_TRY();
// {
//   // C++ implementation code
// }
// CBDB_CATCH_MATCH(std::exception &exp); // optional
// {
//    // specific exception handler
//    error_message.Append("error message: %s", error_message.message());
// }
// CBDB_CATCH_COMM();
// CBDB_CATCH_DEFAULT();
// CBDB_END_TRY();
//
// CBDB_CATCH_MATCH() is optional and can have several match pattern.

// being of a try block w/o explicit handler
#define CBDB_TRY()                               \
  do {                                           \
    bool internal_cbdb_try_throw_error_ = false; \
    cbdb::ErrorMessage error_message;            \
    try {
// begin of a catch block
#define CBDB_CATCH_MATCH(exception_decl) \
  }                                      \
  catch (exception_decl) {               \
    internal_cbdb_try_throw_error_ = true;

#define CBDB_CATCH_COMM()                            \
  }                                                  \
  catch (cbdb::CException & e) {                     \
    internal_cbdb_try_throw_error_ = true;           \
    elog(LOG, "\npax stack trace: \n%s", e.Stack()); \
    ereport(ERROR, errmsg("%s", e.What().c_str()));

// catch c++ exception and rethrow ERROR to C code
// only used by the outer c++ code called by C
#define CBDB_CATCH_DEFAULT() \
  }                          \
  catch (...) {              \
    internal_cbdb_try_throw_error_ = true;

// like PG_FINALLY
#define CBDB_FINALLY(...) \
  }                       \
  {                       \
    do {                  \
      __VA_ARGS__;        \
    } while (0);

// end of a try-catch block
#define CBDB_END_TRY()                                     \
  }                                                        \
  if (internal_cbdb_try_throw_error_) {                    \
    if (error_message.length() == 0)                       \
      error_message.Append("ERROR: %s", __func__);         \
    ereport(ERROR, errmsg("%s", error_message.message())); \
  }                                                        \
  }                                                        \
  while (0)

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
  CBDB_TRY();
  {
    return PaxScanDesc::BeginScan(relation, snapshot, nkeys, key, pscan, flags);
  }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  pg_unreachable();
}

void CCPaxAccessMethod::ScanEnd(TableScanDesc scan) {
  CBDB_TRY();
  { PaxScanDesc::EndScan(scan); }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy PaxScanDesc?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ScanRescan(TableScanDesc scan, ScanKey key,
                                   bool set_params, bool allow_strat,
                                   bool allow_sync, bool allow_pagemode) {
  NOT_IMPLEMENTED_YET;
}

bool CCPaxAccessMethod::ScanGetNextSlot(TableScanDesc scan,
                                        ScanDirection direction,
                                        TupleTableSlot *slot) {
  CBDB_TRY();
  { return PaxScanDesc::ScanGetNextSlot(scan, direction, slot); }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy PaxScanDesc?
  });
  CBDB_END_TRY();

  pg_unreachable();
}

void CCPaxAccessMethod::TupleInsert(Relation relation, TupleTableSlot *slot,
                                    CommandId cid, int options,
                                    BulkInsertState bistate) {
  CBDB_TRY();
  { CPaxInserter::TupleInsert(relation, slot, cid, options, bistate); }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

TM_Result CCPaxAccessMethod::TupleDelete(Relation relation, ItemPointer tid,
                                         CommandId cid, Snapshot snapshot,
                                         Snapshot crosscheck, bool wait,
                                         TM_FailureData *tmfd,
                                         bool changingPart) {
  CBDB_TRY();
  {
    return CPaxDeleter::DeleteTuple(relation, tid, cid, snapshot, crosscheck,
                                    wait, tmfd, changingPart);
  }

  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

TM_Result CCPaxAccessMethod::TupleUpdate(Relation relation, ItemPointer otid,
                                         TupleTableSlot *slot, CommandId cid,
                                         Snapshot snapshot, Snapshot crosscheck,
                                         bool wait, TM_FailureData *tmfd,
                                         LockTupleMode *lockmode,
                                         bool *update_indexes) {
  CBDB_TRY();
  {
    return CPaxUpdater::UpdateTuple(relation, otid, slot, cid, snapshot,
                                    crosscheck, wait, tmfd, lockmode,
                                    update_indexes);
  }

  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanAnalyzeNextBlock(TableScanDesc scan,
                                             BlockNumber blockno,
                                             BufferAccessStrategy bstrategy) {
  return PaxScanDesc::ScanAnalyzeNextBlock(scan, blockno, bstrategy);
}

bool CCPaxAccessMethod::ScanAnalyzeNextTuple(TableScanDesc scan,
                                             TransactionId OldestXmin,
                                             double *liverows, double *deadrows,
                                             TupleTableSlot *slot) {
  return PaxScanDesc::ScanAnalyzeNextTuple(scan, OldestXmin, liverows, deadrows,
                                           slot);
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
  return PaxScanDesc::ScanSampleNextBlock(scan, scanstate);
}

bool CCPaxAccessMethod::ScanSampleNextTuple(TableScanDesc scan,
                                            SampleScanState *scanstate,
                                            TupleTableSlot *slot) {
  return PaxScanDesc::ScanSampleNextTuple(scan, scanstate, slot);
}

void CCPaxAccessMethod::MultiInsert(Relation relation, TupleTableSlot **slots,
                                    int ntuples, CommandId cid, int options,
                                    BulkInsertState bistate) {
  CBDB_TRY();
  {
    CPaxInserter::MultiInsert(relation, slots, ntuples, cid, options, bistate);
  }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::FinishBulkInsert(Relation relation, int options) {
  // Implement Pax dml cleanup for case "create table xxx1 as select * from
  // xxx2", which would not call ExtDmlFini callback function and relies on
  // FinishBulkInsert callback function to cleanup its dml state.
  CBDB_TRY();
  { pax::CPaxInserter::FinishBulkInsert(relation, options); }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ExtDmlInit(Relation rel, CmdType operation) {
  if (!OidIsValid(rel->rd_rel->relam)) {
    Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
    return;
  }
  if (RelationIsPax(rel))
    pax::CPaxDmlStateLocal::instance()->InitDmlState(rel, operation);
}

void CCPaxAccessMethod::ExtDmlFini(Relation rel, CmdType operation) {
  if (!OidIsValid(rel->rd_rel->relam)) {
    Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
    return;
  }
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

    // Create micro-partition file directory for truncate case.
    paxc::CreateMicroPartitionFileDirectory(newrnode, rel->rd_backend,
                                            persistence);
  } else {
    // create pg_pax_blocks_<pax_table_oid>
    cbdb::PaxCreateMicroPartitionTable(rel, newrnode, persistence);
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
  SMgrRelation srel;
  char *srcPath;
  char *dstPath;
  char srcFilePath[PAX_MICROPARTITION_NAME_LENGTH];
  char dstFilePath[PAX_MICROPARTITION_NAME_LENGTH];
  List *fileList = NIL;
  ListCell *listCell;

  srcPath = paxc::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);

  // get micropatition file source folder filename list for copying.
  fileList = paxc::ListDirectory(srcPath);
  if (!fileList || !list_length(fileList)) return;

  // create pg_pax_table relfilenode file and dbid directory under path
  // pg_tblspc/.
  srel = RelationCreateStorage(*newrnode, rel->rd_rel->relpersistence, SMGR_MD);
  smgrclose(srel);

  dstPath = paxc::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
  if (srcPath[0] == '\0' || strlen(srcPath) >= PAX_MICROPARTITION_NAME_LENGTH ||
      dstPath[0] == '\0' || strlen(dstPath) >= PAX_MICROPARTITION_NAME_LENGTH)
    ereport(ERROR,
            (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
             errmsg("MircoPartition file name length mismatch limits.")));

  // create micropartition file destination folder for copying.
  if (MakePGDirectory(dstPath) != 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("RelationCopyData could not create destination "
                           "directory \"%s\": %m for copying",
                           dstPath)));

  foreach (listCell, fileList) {  // NOLINT
    char *filePath = reinterpret_cast<char *>(lfirst(listCell));
    snprintf(srcFilePath, sizeof(srcFilePath), "%s/%s", srcPath, filePath);
    snprintf(dstFilePath, sizeof(dstFilePath), "%s/%s", dstPath, filePath);
    paxc::CopyFile(srcFilePath, dstFilePath);
  }

  // TODO(Tony) : here need to implement pending delete srcPath after set new
  // tablespace.

  pfree(srcPath);
  pfree(dstPath);
  list_free_deep(fileList);
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
    .scan_getnextslot = pax::CCPaxAccessMethod::ScanGetNextSlot,

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

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static uint32 executor_run_ref_count = 0;

void pax_shmem_init() {
  if (prev_shmem_startup_hook) prev_shmem_startup_hook();

  paxc::pax_shmem_startup();
}

static void pax_ExecutorStart(QueryDesc *queryDesc, int eflags) {
  if (prev_ExecutorStart)
    prev_ExecutorStart(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);

  executor_run_ref_count++;
}

static void pax_ExecutorEnd(QueryDesc *queryDesc) {
  if (prev_ExecutorEnd)
    prev_ExecutorEnd(queryDesc);
  else
    standard_ExecutorEnd(queryDesc);
  executor_run_ref_count--;
  Assert(executor_run_ref_count >= 0);
  if (executor_run_ref_count == 0) {
    paxc::release_command_resource();
  }
}

static void pax_xact_callback(XactEvent event, void *arg) {
  if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
      event == XACT_EVENT_PARALLEL_ABORT ||
      event == XACT_EVENT_PARALLEL_COMMIT) {
    if (executor_run_ref_count > 0) {
      executor_run_ref_count = 0;
      paxc::release_command_resource();
    }
  }
}

void _PG_init(void) {
  if (!process_shared_preload_libraries_in_progress) {
    ereport(ERROR, (errmsg("pax must be loaded via shared_preload_libraries")));
    return;
  }

  paxc::pax_shmem_request();

  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = pax_shmem_init;

  prev_ExecutorStart = ExecutorStart_hook;
  ExecutorStart_hook = pax_ExecutorStart;

  prev_ExecutorEnd = ExecutorEnd_hook;
  ExecutorEnd_hook = pax_ExecutorEnd;

  ext_dml_init_hook = pax::CCPaxAccessMethod::ExtDmlInit;
  ext_dml_finish_hook = pax::CCPaxAccessMethod::ExtDmlFini;

  RegisterXactCallback(pax_xact_callback, NULL);

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
