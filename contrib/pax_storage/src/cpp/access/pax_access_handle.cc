#include "access/pax_access_handle.h"

#include "comm/cbdb_api.h"

#include "access/pax_dml_state.h"
#include "access/pax_scanner.h"
#include "access/pax_updater.h"
#include "catalog/pax_aux_table.h"
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

#define RELATION_IS_PAX(rel) \
  (OidIsValid((rel)->rd_rel->relam) && AMOidIsPax((rel)->rd_rel->relam))

// CBDB_TRY();
// {
//   // C++ implementation code
// }
// CBDB_CATCH_MATCH(std::exception &exp); // optional
// {
//    // specific exception handler
//    error_message.Append("error message: %s", error_message.Message());
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
    if (error_message.Length() == 0)                       \
      error_message.Append("ERROR: %s", __func__);         \
    ereport(ERROR, errmsg("%s", error_message.Message())); \
  }                                                        \
  }                                                        \
  while (0)

bool AMOidIsPax(Oid am_oid) {
  HeapTuple tuple;
  Form_pg_am form;
  bool is_pax;

  tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(am_oid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for pg_am.oid = %u", am_oid);

  form = (Form_pg_am)GETSTRUCT(tuple);
  is_pax = strcmp(NameStr(form->amname), "pax") == 0;
  ReleaseSysCache(tuple);

  return is_pax;
}

// reloptions structure and variables.
static relopt_kind self_relopt_kind;
static const relopt_parse_elt kSelfReloptTab[] = {
    {"compresslevel", RELOPT_TYPE_INT, offsetof(PaxOptions, compress_level)},
    {"compresstype", RELOPT_TYPE_STRING, offsetof(PaxOptions, compress_type)},
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
    return PaxScanDesc::BeginScan(relation, snapshot, nkeys, key, pscan, flags,
                                  nullptr);
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

TableScanDesc CCPaxAccessMethod::ScanExtractColumns(
    Relation rel, Snapshot snapshot, ParallelTableScanDesc parallel_scan,
    List *targetlist, List *qual, uint32 flags) {
  CBDB_TRY();
  {
    return pax::PaxScanDesc::BeginScanExtractColumns(
        rel, snapshot, parallel_scan, targetlist, qual, flags);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

void CCPaxAccessMethod::RelationSetNewFilenode(Relation rel,
                                               const RelFileNode *newrnode,
                                               char persistence,
                                               TransactionId *freeze_xid,
                                               MultiXactId *minmulti) {
  CBDB_TRY();
  {
    *freeze_xid = *minmulti = InvalidTransactionId;
    pax::CCPaxAuxTable::PaxAuxRelationSetNewFilenode(rel, newrnode,
                                                     persistence);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
void CCPaxAccessMethod::RelationNontransactionalTruncate(Relation rel) {
  CBDB_TRY();
  { pax::CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(rel); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::RelationCopyData(Relation rel,
                                         const RelFileNode *newrnode) {
  CBDB_TRY();
  { pax::CCPaxAuxTable::PaxAuxRelationCopyData(rel, newrnode); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::RelationFileUnlink(RelFileNodeBackend rnode) {
  CBDB_TRY();
  {
    pax::CCPaxAuxTable::PaxAuxRelationFileUnlink(rnode.node, rnode.backend,
                                                 true);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ScanRescan(TableScanDesc scan, ScanKey /*key*/,
                                   bool /*set_params*/, bool /*allow_strat*/,
                                   bool /*allow_sync*/,
                                   bool /*allow_pagemode*/) {
  CBDB_TRY();
  { pax::PaxScanDesc::ReScan(scan); }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

bool CCPaxAccessMethod::ScanGetNextSlot(TableScanDesc scan,
                                        ScanDirection /*direction*/,
                                        TupleTableSlot *slot) {
  CBDB_TRY();
  { return PaxScanDesc::ScanGetNextSlot(scan, slot); }
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
  {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    CPaxInserter::TupleInsert(relation, slot, cid, options, bistate);
    MemoryContextSwitchTo(old_ctx);
  }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

TM_Result CCPaxAccessMethod::TupleDelete(Relation relation, ItemPointer tid,
                                         CommandId cid, Snapshot snapshot,
                                         Snapshot /*crosscheck*/, bool /*wait*/,
                                         TM_FailureData *tmfd,
                                         bool /*changing_part*/) {
  CBDB_TRY();
  { return CPaxDeleter::DeleteTuple(relation, tid, cid, snapshot, tmfd); }

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
    MemoryContext old_ctx;
    TM_Result result;

    Assert(cbdb::pax_memory_context);
    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    result = CPaxUpdater::UpdateTuple(relation, otid, slot, cid, snapshot,
                                      crosscheck, wait, tmfd, lockmode,
                                      update_indexes);
    MemoryContextSwitchTo(old_ctx);
    return result;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanAnalyzeNextBlock(
    TableScanDesc scan, BlockNumber blockno,
    BufferAccessStrategy /*bstrategy*/) {
  CBDB_TRY();
  { return PaxScanDesc::ScanAnalyzeNextBlock(scan, blockno); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanAnalyzeNextTuple(TableScanDesc scan,
                                             TransactionId /*oldest_xmin*/,
                                             double *liverows, double *deadrows,
                                             TupleTableSlot *slot) {
  CBDB_TRY();
  { return PaxScanDesc::ScanAnalyzeNextTuple(scan, liverows, deadrows, slot); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanBitmapNextBlock(TableScanDesc /*scan*/,
                                            TBMIterateResult * /*tbmres*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanBitmapNextTuple(TableScanDesc /*scan*/,
                                            TBMIterateResult * /*tbmres*/,
                                            TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanSampleNextBlock(TableScanDesc scan,
                                            SampleScanState *scanstate) {
  CBDB_TRY();
  { return PaxScanDesc::ScanSampleNextBlock(scan, scanstate); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanSampleNextTuple(TableScanDesc scan,
                                            SampleScanState * /*scanstate*/,
                                            TupleTableSlot *slot) {
  CBDB_TRY();
  { return PaxScanDesc::ScanSampleNextTuple(scan, slot); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

void CCPaxAccessMethod::MultiInsert(Relation relation, TupleTableSlot **slots,
                                    int ntuples, CommandId cid, int options,
                                    BulkInsertState bistate) {
  CBDB_TRY();
  {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    CPaxInserter::MultiInsert(relation, slots, ntuples, cid, options, bistate);
    MemoryContextSwitchTo(old_ctx);
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
  {
    // no need switch memory context
    // cause it just call dml finish
    pax::CPaxInserter::FinishBulkInsert(relation, options);
  }
  CBDB_CATCH_COMM();
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ExtDmlInit(Relation rel, CmdType operation) {
  if (RELATION_IS_PAX(rel)) {
    pax::CPaxDmlStateLocal::Instance()->InitDmlState(rel, operation);
  }
}

void CCPaxAccessMethod::ExtDmlFini(Relation rel, CmdType operation) {
  if (RELATION_IS_PAX(rel)) {
    pax::CPaxDmlStateLocal::Instance()->FinishDmlState(rel, operation);
  }
}

}  // namespace pax
// END of C++ implementation

// access methods that are implemented in C
namespace paxc {
const TupleTableSlotOps *PaxAccessMethod::SlotCallbacks(
    Relation /*rel*/) noexcept {
  return &TTSOpsVirtual;
}

Size PaxAccessMethod::ParallelscanEstimate(Relation /*rel*/) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

Size PaxAccessMethod::ParallelscanInitialize(Relation /*rel*/,
                                             ParallelTableScanDesc /*pscan*/) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

void PaxAccessMethod::ParallelscanReinitialize(
    Relation /*rel*/, ParallelTableScanDesc /*pscan*/) {
  NOT_IMPLEMENTED_YET;
}

struct IndexFetchTableData *PaxAccessMethod::IndexFetchBegin(Relation /*rel*/) {
  NOT_SUPPORTED_YET;
  return nullptr;
}

void PaxAccessMethod::IndexFetchEnd(IndexFetchTableData * /*data*/) {
  NOT_SUPPORTED_YET;
}

void PaxAccessMethod::IndexFetchReset(IndexFetchTableData * /*data*/) {
  NOT_SUPPORTED_YET;
}

bool PaxAccessMethod::IndexFetchTuple(struct IndexFetchTableData * /*scan*/,
                                      ItemPointer /*tid*/,
                                      Snapshot /*snapshot*/,
                                      TupleTableSlot * /*slot*/,
                                      bool * /*call_again*/,
                                      bool * /*all_dead*/) {
  NOT_SUPPORTED_YET;
  return false;
}

void PaxAccessMethod::TupleInsertSpeculative(Relation /*relation*/,
                                             TupleTableSlot * /*slot*/,
                                             CommandId /*cid*/, int /*options*/,
                                             BulkInsertState /*bistate*/,
                                             uint32 /*spec_token*/) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::TupleCompleteSpeculative(Relation /*relation*/,
                                               TupleTableSlot * /*slot*/,
                                               uint32 /*spec_token*/,
                                               bool /*succeeded*/) {
  NOT_IMPLEMENTED_YET;
}

TM_Result PaxAccessMethod::TupleLock(Relation /*relation*/, ItemPointer /*tid*/,
                                     Snapshot /*snapshot*/,
                                     TupleTableSlot * /*slot*/,
                                     CommandId /*cid*/, LockTupleMode /*mode*/,
                                     LockWaitPolicy /*wait_policy*/,
                                     uint8 /*flags*/,
                                     TM_FailureData * /*tmfd*/) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
}

bool PaxAccessMethod::TupleFetchRowVersion(Relation /*relation*/,
                                           ItemPointer /*tid*/,
                                           Snapshot /*snapshot*/,
                                           TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool PaxAccessMethod::TupleTidValid(TableScanDesc /*scan*/,
                                    ItemPointer /*tid*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

void PaxAccessMethod::TupleGetLatestTid(TableScanDesc /*sscan*/,
                                        ItemPointer /*tid*/) {
  NOT_SUPPORTED_YET;
}

bool PaxAccessMethod::TupleSatisfiesSnapshot(Relation /*rel*/,
                                             TupleTableSlot * /*slot*/,
                                             Snapshot /*snapshot*/) {
  NOT_IMPLEMENTED_YET;
  return true;
}

TransactionId PaxAccessMethod::IndexDeleteTuples(
    Relation /*rel*/, TM_IndexDeleteOp * /*delstate*/) {
  NOT_SUPPORTED_YET;
  return 0;
}

/*
 * Used by rebuild_relation, like CLUSTER, VACUUM FULL, etc.
 *
 * PAX does not have dead tuples, but the core framework requires
 * to implement this callback to do CLUSTER/VACUUM FULL/etc.
 * PAX may have re-organize semantics for this function.
 *
 * TODO: how to split the set of micro-partitions to several QE handlers.
 */
void PaxAccessMethod::RelationCopyForCluster(
    Relation old_heap, Relation new_heap, Relation /*old_index*/,
    bool /*use_sort*/, TransactionId /*oldest_xmin*/,
    TransactionId * /*xid_cutoff*/, MultiXactId * /*multi_cutoff*/,
    double * /*num_tuples*/, double * /*tups_vacuumed*/,
    double * /*tups_recently_dead*/) {
  Assert(RELATION_IS_PAX(old_heap));
  if (!RELATION_IS_PAX(new_heap))
    ereport(ERROR, (errmsg("PAX: can't convert pax to non-pax table")));

  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::RelationVacuum(Relation /*onerel*/,
                                     VacuumParams * /*params*/,
                                     BufferAccessStrategy /*bstrategy*/) {
  /* PAX: micro-partitions have no dead tuples, so vacuum is empty */
}

uint64 PaxAccessMethod::RelationSize(Relation rel, ForkNumber fork_number) {
  Oid pax_aux_oid;
  Relation pax_aux_rel;
  TupleDesc aux_tup_desc;
  HeapTuple aux_tup;
  SysScanDesc aux_scan;
  uint64 pax_size = 0;

  if (fork_number != MAIN_FORKNUM) return 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  GetPaxTablesEntryAttributes(rel->rd_id, &pax_aux_oid, NULL, NULL);

  // Scan pg_pax_blocks_xxx to calculate size of micro partition
  pax_aux_rel = heap_open(pax_aux_oid, AccessShareLock);
  aux_tup_desc = RelationGetDescr(pax_aux_rel);

  aux_scan = systable_beginscan(pax_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(aux_tup = systable_getnext(aux_scan))) {
    bool isnull = false;
    // TODO(chenhongjie): Exactly what is needed and being obtained is
    // compressed size. Later, when the aux table supports size attributes
    // before/after compression, we need to distinguish two attributes by names.
    Datum tup_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE, aux_tup_desc, &isnull);

    Assert(!isnull);
    pax_size += DatumGetUInt32(tup_datum);
  }

  systable_endscan(aux_scan);
  heap_close(pax_aux_rel, AccessShareLock);

  return pax_size;
}

bool PaxAccessMethod::RelationNeedsToastTable(Relation /*rel*/) {
  // PAX never used the toasting, don't create the toast table from Cloudberry 7

  return false;
}

// Similar to the case of AO and AOCS tables, PAX table has auxiliary tables,
// size can be read directly from the auxiliary table, and there is not much
// space for optimization in estimating relsize. So this function is implemented
// in the same way as pax_relation_size().
void PaxAccessMethod::EstimateRelSize(Relation rel, int32 * /*attr_widths*/,
                                      BlockNumber *pages, double *tuples,
                                      double *allvisfrac) {
  Oid pax_aux_oid;
  Relation pax_aux_rel;
  TupleDesc aux_tup_desc;
  HeapTuple aux_tup;
  SysScanDesc aux_scan;
  uint32 total_tuples = 0;
  uint64 pax_size = 0;

  // Even an empty table takes at least one page,
  // but number of tuples for an empty table could be 0.
  *tuples = 0;
  *pages = 1;
  // index-only scan is not supported in PAX
  *allvisfrac = 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  GetPaxTablesEntryAttributes(rel->rd_id, &pax_aux_oid, NULL, NULL);

  // Scan pg_pax_blocks_xxx to get attributes
  pax_aux_rel = heap_open(pax_aux_oid, AccessShareLock);
  aux_tup_desc = RelationGetDescr(pax_aux_rel);

  aux_scan = systable_beginscan(pax_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(aux_tup = systable_getnext(aux_scan))) {
    Datum pttupcount_datum;
    Datum ptblocksize_datum;
    bool isnull = false;

    pttupcount_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT, aux_tup_desc, &isnull);
    Assert(!isnull);
    total_tuples += DatumGetUInt32(pttupcount_datum);

    isnull = false;
    // TODO(chenhongjie): Exactly what we want to get here is uncompressed size,
    // but what we're getting is compressed size. Later, when the aux table
    // supports size attributes before/after compression, this needs to
    // be corrected.
    ptblocksize_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE, aux_tup_desc, &isnull);

    Assert(!isnull);
    pax_size += DatumGetUInt32(ptblocksize_datum);
  }

  systable_endscan(aux_scan);
  heap_close(pax_aux_rel, AccessShareLock);

  *tuples = static_cast<double>(total_tuples);
  *pages = RelationGuessNumberOfBlocksFromSize(pax_size);
}

double PaxAccessMethod::IndexBuildRangeScan(
    Relation /*heap_relation*/, Relation /*index_relation*/,
    IndexInfo * /*index_info*/, bool /*allow_sync*/, bool /*anyvisible*/,
    bool /*progress*/, BlockNumber /*start_blockno*/, BlockNumber /*numblocks*/,
    IndexBuildCallback /*callback*/, void * /*callback_state*/,
    TableScanDesc /*scan*/) {
  NOT_SUPPORTED_YET;
  return 0.0;
}

void PaxAccessMethod::IndexValidateScan(Relation /*heap_relation*/,
                                        Relation /*index_relation*/,
                                        IndexInfo * /*index_info*/,
                                        Snapshot /*snapshot*/,
                                        ValidateIndexState * /*state*/) {
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
bytea *PaxAccessMethod::Amoptions(Datum reloptions, char /*relkind*/,
                                  bool validate) {
  void *rdopts;

  rdopts = build_reloptions(reloptions, validate, self_relopt_kind,
                            sizeof(PaxOptions), kSelfReloptTab,
                            lengthof(kSelfReloptTab));
  // adjust string values
  PAX_COPY_OPT(rdopts, storage_format);
  PAX_COPY_OPT(rdopts, compress_type);

  return reinterpret_cast<bytea *>(rdopts);
}
#undef PAX_COPY_OPT

void PaxAccessMethod::SwapRelationFiles(Oid relid1, Oid relid2,
                                        TransactionId frozen_xid,
                                        MultiXactId cutoff_multi) {
  HeapTuple tuple1;
  HeapTuple tuple2;
  Relation pax_rel;

  Oid b_relid1;
  Oid b_relid2;

  pax_rel = table_open(PaxTablesRelationId, RowExclusiveLock);

  tuple1 = SearchSysCacheCopy1(PAXTABLESID, relid1);
  if (!HeapTupleIsValid(tuple1))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cache lookup failed with relid=%u for aux relation "
                           "in pg_pax_tables.",
                           relid1)));

  tuple2 = SearchSysCacheCopy1(PAXTABLESID, relid2);
  if (!HeapTupleIsValid(tuple2))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cache lookup failed with relid=%u for aux relation "
                           "in pg_pax_tables.",
                           relid2)));

  // swap the entries
  {
    Form_pg_pax_tables form1;
    Form_pg_pax_tables form2;

    int16 temp_compresslevel;
    NameData temp_compresstype;

    form1 = (Form_pg_pax_tables)GETSTRUCT(tuple1);
    form2 = (Form_pg_pax_tables)GETSTRUCT(tuple2);

    Assert(((Form_pg_pax_tables)GETSTRUCT(tuple1))->relid == relid1);
    Assert(((Form_pg_pax_tables)GETSTRUCT(tuple2))->relid == relid2);

    b_relid1 = form1->blocksrelid;
    b_relid2 = form2->blocksrelid;

    memcpy(&temp_compresstype, &form1->compresstype, sizeof(NameData));
    memcpy(&form1->compresstype, &form2->compresstype, sizeof(NameData));
    memcpy(&form2->compresstype, &temp_compresstype, sizeof(NameData));

    temp_compresslevel = form1->compresslevel;
    form1->compresslevel = form2->compresslevel;
    form2->compresslevel = temp_compresslevel;
  }

  {
    CatalogIndexState indstate;

    indstate = CatalogOpenIndexes(pax_rel);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple1->t_self, tuple1, indstate);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple2->t_self, tuple2, indstate);
    CatalogCloseIndexes(indstate);
  }

  table_close(pax_rel, NoLock);

  /* swap relation files for aux table */
  {
    Relation b_rel1;
    Relation b_rel2;

    b_rel1 = relation_open(b_relid1, AccessExclusiveLock);
    b_rel2 = relation_open(b_relid2, AccessExclusiveLock);

    swap_relation_files(b_relid1, b_relid2, false, /* target_is_pg_class */
                        true,                      /* swap_toast_by_content */
                        true,                      /*swap_stats */
                        true,                      /* is_internal */
                        frozen_xid, cutoff_multi, NULL);

    relation_close(b_rel1, NoLock);
    relation_close(b_rel2, NoLock);
  }
}

}  // namespace paxc
// END of C implementation

extern "C" {

static const TableAmRoutine kPaxColumnMethods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = paxc::PaxAccessMethod::SlotCallbacks,
    .scan_begin = pax::CCPaxAccessMethod::ScanBegin,
    .scan_begin_extractcolumns = pax::CCPaxAccessMethod::ScanExtractColumns,
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

    .relation_set_new_filenode = pax::CCPaxAccessMethod::RelationSetNewFilenode,
    .relation_nontransactional_truncate =
        pax::CCPaxAccessMethod::RelationNontransactionalTruncate,
    .relation_copy_data = pax::CCPaxAccessMethod::RelationCopyData,
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
    .swap_relation_files = paxc::PaxAccessMethod::SwapRelationFiles,
};

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);
Datum pax_tableam_handler(PG_FUNCTION_ARGS) {  // NOLINT
  PG_RETURN_POINTER(&kPaxColumnMethods);
}

static void PaxValidateStorageFormat(const char *value) {
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

static void PaxValidateCompresstype(const char *value) {
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
static ExecutorStart_hook_type prev_executor_start = NULL;
static ExecutorEnd_hook_type prev_executor_end = NULL;
static uint32 executor_run_ref_count = 0;

void PaxShmemInit() {
  if (prev_shmem_startup_hook) prev_shmem_startup_hook();

  paxc::paxc_shmem_startup();
}

static void PaxExecutorStart(QueryDesc *query_desc, int eflags) {
  if (prev_executor_start)
    prev_executor_start(query_desc, eflags);
  else
    standard_ExecutorStart(query_desc, eflags);

  executor_run_ref_count++;
}

static void PaxExecutorEnd(QueryDesc *query_desc) {
  if (prev_executor_end)
    prev_executor_end(query_desc);
  else
    standard_ExecutorEnd(query_desc);

  executor_run_ref_count--;
  Assert(executor_run_ref_count >= 0);
  if (executor_run_ref_count == 0) {
    paxc::release_command_resource();
  }
}

static void PaxXactCallback(XactEvent event, void * /*arg*/) {
  if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
      event == XACT_EVENT_PARALLEL_ABORT ||
      event == XACT_EVENT_PARALLEL_COMMIT) {
    if (executor_run_ref_count > 0) {
      executor_run_ref_count = 0;
      paxc::release_command_resource();
    }
  }
}

void _PG_init(void) {  // NOLINT
  if (!process_shared_preload_libraries_in_progress) {
    ereport(ERROR, (errmsg("pax must be loaded via shared_preload_libraries")));
    return;
  }

  paxc::paxc_shmem_request();

  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = PaxShmemInit;

  prev_executor_start = ExecutorStart_hook;
  ExecutorStart_hook = PaxExecutorStart;

  prev_executor_end = ExecutorEnd_hook;
  ExecutorEnd_hook = PaxExecutorEnd;

  ext_dml_init_hook = pax::CCPaxAccessMethod::ExtDmlInit;
  ext_dml_finish_hook = pax::CCPaxAccessMethod::ExtDmlFini;
  file_unlink_hook = pax::CCPaxAccessMethod::RelationFileUnlink;

  RegisterXactCallback(PaxXactCallback, NULL);

  self_relopt_kind = add_reloption_kind();
  add_string_reloption(self_relopt_kind, "storage_format", "pax storage format",
                       "orc", PaxValidateStorageFormat, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, "compresstype", "pax compress type",
                       PAX_DEFAULT_COMPRESSTYPE, PaxValidateCompresstype,
                       AccessExclusiveLock);
  add_int_reloption(self_relopt_kind, "compresslevel", "pax compress level",
                    PAX_DEFAULT_COMPRESSLEVEL, AO_MIN_COMPRESSLEVEL,
                    AO_MAX_COMPRESSLEVEL, AccessExclusiveLock);
}
}  // extern "C"
