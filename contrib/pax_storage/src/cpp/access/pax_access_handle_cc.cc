#include "access/pax_access.h"
#include "access/pax_access_handle.h"

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
  pax::CPaxAccess::PaxRescan(scan);
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
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanAnalyzeNextTuple(TableScanDesc scan,
                                             TransactionId OldestXmin,
                                             double *liverows, double *deadrows,
                                             TupleTableSlot *slot) {
  NOT_IMPLEMENTED_YET;
  return false;
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
  NOT_IMPLEMENTED_YET;
  return false;
}

bool CCPaxAccessMethod::ScanSampleNextTuple(TableScanDesc scan,
                                            SampleScanState *scanstate,
                                            TupleTableSlot *slot) {
  NOT_IMPLEMENTED_YET;
  return false;
}

}  // namespace pax
