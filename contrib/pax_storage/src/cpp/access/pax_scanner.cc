#include "access/pax_scanner.h"

#include "access/pax_access_handle.h"
#include "catalog/table_metadata.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"
#include "storage/pax.h"
#include "storage/pax_buffer.h"

namespace pax {

TableScanDesc PaxScanDesc::BeginScan(const Relation relation,
                                     const Snapshot snapshot, const int nkeys,
                                     const struct ScanKeyData *key,
                                     const ParallelTableScanDesc pscan,
                                     const uint32 flags) {
  PaxScanDesc *desc;

  StaticAssertStmt(
      offsetof(PaxScanDesc, rs_base) == 0,
      "rs_base should be the first field and aligned to the object address");

  desc = new PaxScanDesc();

  Assert(reinterpret_cast<PaxScanDesc *>(&desc->rs_base) == desc);

  memset(&desc->rs_base, 0, sizeof(desc->rs_base));
  desc->rs_base.rs_rd = relation;
  desc->rs_base.rs_snapshot = snapshot;
  desc->rs_base.rs_nkeys = nkeys;
  desc->rs_base.rs_flags = flags;
  desc->rs_base.rs_parallel = pscan;
  desc->key_ = key;
  desc->reused_buffer = new DataBuffer<char>(32 * 1024 * 1024);  // 32mb

  // build reader
  TableMetadata *meta_info;
  meta_info = TableMetadata::Create(relation, snapshot);

  FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReader *micro_partition_reader =
      new OrcIteratorReader(file_system);
  micro_partition_reader->SetReadBuffer(desc->reused_buffer);

  desc->reader_ =
      new TableReader(micro_partition_reader, meta_info->NewIterator());
  desc->reader_->Open();

  return &desc->rs_base;
}

void PaxScanDesc::EndScan(TableScanDesc scan) {
  PaxScanDesc *desc = to_desc(scan);

  Assert(desc->reader_);
  desc->reader_->Close();
  delete desc->reused_buffer;
  delete desc->reader_;
  delete desc;
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(TableScanDesc scan, ScanKey key, bool set_params,
                         bool allow_strat, bool allow_sync,
                         bool allow_pagemode) {
  PaxScanDesc *desc = to_desc(scan);
  Assert(desc && desc->reader_);
  desc->reader_->ReOpen();
}

bool PaxScanDesc::ScanGetNextSlot(TableScanDesc scan,
                                  const ScanDirection direction,
                                  TupleTableSlot *slot) {
  PaxScanDesc *desc = to_desc(scan);
  CTupleSlot cslot(slot);
  return desc->reader_->ReadTuple(&cslot);
}

bool PaxScanDesc::ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno,
                                       BufferAccessStrategy bstrategy) {
  PaxScanDesc *desc = to_desc(scan);
  desc->targetTupleId = blockno;

  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TableScanDesc scan,
                                       TransactionId OldestXmin,
                                       double *liverows, double *deadrows,
                                       TupleTableSlot *slot) {
  PaxScanDesc *desc = to_desc(scan);
  bool ret = false;

  Assert(*deadrows == 0);  // not dead rows in pax latest snapshot

  // skip several tuples if they are not sampling target.
  ret = desc->SeekTuple(desc->targetTupleId, &(desc->nextTupleId));

  if (!ret) {
    return false;
  }

  ret = PaxScanDesc::ScanGetNextSlot(scan, ForwardScanDirection, slot);
  desc->nextTupleId++;
  if (ret) {
    *liverows += 1;
  }

  // Unlike heap table, latest pax snapshot does not contain deadrows,
  // so `false` value of ret indicate that no more tuple to fetch,
  // and just return directly.
  return ret;
}

bool PaxScanDesc::ScanSampleNextBlock(TableScanDesc scan,
                                      SampleScanState *scanstate) {
  PaxScanDesc *desc = to_desc(scan);
  TsmRoutine *tsm = scanstate->tsmroutine;
  BlockNumber blockno = 0;
  BlockNumber pages = 0;
  double totaltuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;

  if (desc->totalTuples == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(scan->rs_rd, &attrwidths, &pages,
                                           &totaltuples, &allvisfrac);
    desc->totalTuples = totaltuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, desc->totalTuples);
  else
    blockno = system_nextsampleblock(scanstate, desc->totalTuples);

  if (!BlockNumberIsValid(blockno)) return false;

  desc->fetchTupleId = blockno;
  return true;
}

bool PaxScanDesc::ScanSampleNextTuple(TableScanDesc scan,
                                      SampleScanState *scanstate,
                                      TupleTableSlot *slot) {
  PaxScanDesc *desc = to_desc(scan);
  bool ret = false;

  // skip several tuples if they are not sampling target.
  ret = desc->SeekTuple(desc->fetchTupleId, &desc->nextTupleId);

  if (!ret) return false;

  ret = PaxScanDesc::ScanGetNextSlot(scan, ForwardScanDirection, slot);
  desc->nextTupleId++;

  return ret;
}

PaxScanDesc::~PaxScanDesc() {}
}  // namespace pax
