#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class PaxAccessMethod final {
 public:
  static const TupleTableSlotOps *SlotCallbacks(Relation rel) noexcept;

  static void ScanSetTidrange(TableScanDesc scan, ItemPointer mintid,
                              ItemPointer maxtid);
  static void ScanGetnextslotTidrange(TableScanDesc scan,
                                      ScanDirection direction,
                                      TupleTableSlot *slot);

  /* Parallel table scan related functions. */
  static Size ParallelscanEstimate(Relation rel);
  static Size ParallelscanInitialize(Relation rel, ParallelTableScanDesc pscan);
  static void ParallelscanReinitialize(Relation rel,
                                       ParallelTableScanDesc pscan);

  /* Index Scan Callbacks, unsupported yet */
  static struct IndexFetchTableData *IndexFetchBegin(Relation rel);
  static void IndexFetchEnd(struct IndexFetchTableData *data);
  static void IndexFetchReset(struct IndexFetchTableData *data);
  static bool IndexFetchTuple(struct IndexFetchTableData *scan, ItemPointer tid,
                              Snapshot snapshot, TupleTableSlot *slot,
                              bool *call_again, bool *all_dead);

  /* Callbacks for non-modifying operations on individual tuples */
  static bool TupleFetchRowVersion(Relation relation, ItemPointer tid,
                                   Snapshot snapshot, TupleTableSlot *slot);
  static bool TupleTidValid(TableScanDesc scan, ItemPointer tid);
  static void TupleGetLatestTid(TableScanDesc sscan, ItemPointer tid);
  static bool TupleSatisfiesSnapshot(Relation rel, TupleTableSlot *slot,
                                     Snapshot snapshot);
  static TransactionId IndexDeleteTuples(Relation rel,
                                         TM_IndexDeleteOp *delstate);

  static void RelationSetNewFilenode(Relation rel, const RelFileNode *newrnode,
                                     char persistence, TransactionId *freezeXid,
                                     MultiXactId *minmulti);
  static void RelationNontransactionalTruncate(Relation rel);

  static bool RelationNeedsToastTable(Relation rel);
  static uint64 RelationSize(Relation rel, ForkNumber forkNumber);
  static void EstimateRelSize(Relation rel, int32 *attr_widths,
                              BlockNumber *pages, double *tuples,
                              double *allvisfrac);

  /* unsupported DML now, may move to CCPaxAccessMethod */
  static void TupleInsertSpeculative(Relation relation, TupleTableSlot *slot,
                                     CommandId cid, int options,
                                     BulkInsertState bistate, uint32 specToken);
  static void TupleCompleteSpeculative(Relation relation, TupleTableSlot *slot,
                                       uint32 specToken, bool succeeded);
  static void MultiInsert(Relation relation, TupleTableSlot **slots,
                          int ntuples, CommandId cid, int options,
                          BulkInsertState bistate);
  static TM_Result TupleLock(Relation relation, ItemPointer tid,
                             Snapshot snapshot, TupleTableSlot *slot,
                             CommandId cid, LockTupleMode mode,
                             LockWaitPolicy wait_policy, uint8 flags,
                             TM_FailureData *tmfd);
  static void FinishBulkInsert(Relation relation, int options);

  static void RelationCopyData(Relation rel, const RelFileNode *newrnode);
  static void RelationCopyForCluster(Relation OldHeap, Relation NewHeap,
                                     Relation OldIndex, bool use_sort,
                                     TransactionId OldestXmin,
                                     TransactionId *xid_cutoff,
                                     MultiXactId *multi_cutoff,
                                     double *num_tuples, double *tups_vacuumed,
                                     double *tups_recently_dead);

  static void RelationVacuum(Relation onerel, VacuumParams *params,
                             BufferAccessStrategy bstrategy);
  static double IndexBuildRangeScan(
      Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
      bool allow_sync, bool anyvisible, bool progress,
      BlockNumber start_blockno, BlockNumber numblocks,
      IndexBuildCallback callback, void *callback_state, TableScanDesc scan);
  static void IndexValidateScan(Relation heapRelation, Relation indexRelation,
                                IndexInfo *indexInfo, Snapshot snapshot,
                                ValidateIndexState *state);
};

class CCPaxAccessMethod final {
 public:
  static TableScanDesc ScanBegin(Relation rel, Snapshot snapshot, int nkeys,
                                 struct ScanKeyData *key,
                                 ParallelTableScanDesc pscan, uint32 flags);
  static void ScanEnd(TableScanDesc scan);
  static void ScanRescan(TableScanDesc scan, struct ScanKeyData *key,
                         bool set_params, bool allow_strat, bool allow_sync,
                         bool allow_pagemode);
  static bool ScanGetnextslot(TableScanDesc scan, ScanDirection direction,
                              TupleTableSlot *slot);

  /* Manipulations of physical tuples. */
  static void TupleInsert(Relation relation, TupleTableSlot *slot,
                          CommandId cid, int options, BulkInsertState bistate);
  static TM_Result TupleDelete(Relation relation, ItemPointer tid,
                               CommandId cid, Snapshot snapshot,
                               Snapshot crosscheck, bool wait,
                               TM_FailureData *tmfd, bool changingPart);
  static TM_Result TupleUpdate(Relation relation, ItemPointer otid,
                               TupleTableSlot *slot, CommandId cid,
                               Snapshot snapshot, Snapshot crosscheck,
                               bool wait, TM_FailureData *tmfd,
                               LockTupleMode *lockmode, bool *update_indexes);

  static bool ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno,
                                   BufferAccessStrategy bstrategy);
  static bool ScanAnalyzeNextTuple(TableScanDesc scan, TransactionId OldestXmin,
                                   double *liverows, double *deadrows,
                                   TupleTableSlot *slot);
  static bool ScanBitmapNextBlock(TableScanDesc scan, TBMIterateResult *tbmres);
  static bool ScanBitmapNextTuple(TableScanDesc scan, TBMIterateResult *tbmres,
                                  TupleTableSlot *slot);
  static bool ScanSampleNextBlock(TableScanDesc scan,
                                  SampleScanState *scanstate);
  static bool ScanSampleNextTuple(TableScanDesc scan,
                                  SampleScanState *scanstate,
                                  TupleTableSlot *slot);

  // DML init/fini hooks
  static void ExtDmlInit(Relation rel, CmdType operation);
  static void ExtDmlFini(Relation rel, CmdType operation);
};

}  // namespace pax
