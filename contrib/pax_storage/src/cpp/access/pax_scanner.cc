#include "access/pax_scanner.h"
#include "access/pax_access_handle.h"
#include "catalog/table_metadata.h"
#include "comm/paxc_utils.h"
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
      offsetof(PaxScanDesc, rs_base_) == 0,
      "rs_base should be the first field and aligned to the object address");

  desc = new PaxScanDesc();

  memset(&desc->rs_base_, 0, sizeof(desc->rs_base_));
  desc->rs_base_.rs_rd = relation;
  desc->rs_base_.rs_snapshot = snapshot;
  desc->rs_base_.rs_nkeys = nkeys;
  desc->rs_base_.rs_flags = flags;
  desc->rs_base_.rs_parallel = pscan;
  desc->key_ = key;
  desc->reused_buffer_ = new DataBuffer<char>(32 * 1024 * 1024);  // 32mb

  // init shared memory
  cbdb::InitCommandResource();
  // build reader
  TableMetadata *meta_info;
  meta_info = TableMetadata::Create(relation, snapshot);

  FileSystem *file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReader *micro_partition_reader =
      new OrcIteratorReader(file_system);
  micro_partition_reader->SetReadBuffer(desc->reused_buffer_);

  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = true;
  reader_options.rel_oid = desc->rs_base_.rs_rd->rd_id;

  desc->reader_ = new TableReader(micro_partition_reader,
                                  meta_info->NewIterator(), reader_options);
  desc->reader_->Open();

  return &desc->rs_base_;
}

void PaxScanDesc::EndScan(TableScanDesc scan) {
  PaxScanDesc *desc = to_desc(scan);

  Assert(desc->reader_);
  desc->reader_->Close();
  delete desc->reused_buffer_;
  delete desc->reader_;
  delete desc;
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(TableScanDesc scan) {
  PaxScanDesc *desc = to_desc(scan);
  Assert(desc && desc->reader_);
  desc->reader_->ReOpen();
}

bool PaxScanDesc::ScanGetNextSlot(TableScanDesc scan,
                                  TupleTableSlot *slot) {
  PaxScanDesc *desc = to_desc(scan);
  CTupleSlot cslot(slot);
  return desc->reader_->ReadTuple(&cslot);
}

bool PaxScanDesc::ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno) {
  PaxScanDesc *desc = to_desc(scan);
  desc->target_tuple_id_ = blockno;

  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TableScanDesc scan,
                                       double *liverows, double *deadrows,
                                       TupleTableSlot *slot) {
  cbdb::Unused(deadrows);
  PaxScanDesc *desc = to_desc(scan);
  bool ret = false;

  Assert(*deadrows == 0);  // not dead rows in pax latest snapshot

  // skip several tuples if they are not sampling target.
  ret = desc->SeekTuple(desc->target_tuple_id_, &(desc->next_tuple_id_));

  if (!ret) {
    return false;
  }

  ret = PaxScanDesc::ScanGetNextSlot(scan, slot);
  desc->next_tuple_id_++;
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
  double total_tuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;

  if (desc->total_tuples_ == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(scan->rs_rd, &attrwidths, &pages,
                                           &total_tuples, &allvisfrac);
    desc->total_tuples_ = total_tuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, desc->total_tuples_);
  else
    blockno = system_nextsampleblock(scanstate, desc->total_tuples_);

  if (!BlockNumberIsValid(blockno)) return false;

  desc->fetch_tuple_id_ = blockno;
  return true;
}

bool PaxScanDesc::ScanSampleNextTuple(TableScanDesc scan,
                                      TupleTableSlot *slot) {
  PaxScanDesc *desc = to_desc(scan);
  bool ret = false;

  // skip several tuples if they are not sampling target.
  ret = desc->SeekTuple(desc->fetch_tuple_id_, &desc->next_tuple_id_);

  if (!ret) return false;

  ret = PaxScanDesc::ScanGetNextSlot(scan, slot);
  desc->next_tuple_id_++;

  return ret;
}

uint32 PaxScanDesc::GetMicroPartitionNumber() const {
  return reader_->GetMicroPartitionNumber();
}

uint32 PaxScanDesc::GetCurrentMicroPartitionTupleNumber() const {
  return reader_->GetCurrentMicroPartitionTupleNumber();
}

bool PaxScanDesc::SeekTuple(const uint64 target_tuple_id,
                            uint64 *next_tuple_id) {
  return reader_->SeekTuple(target_tuple_id, next_tuple_id);
}

}  // namespace pax
