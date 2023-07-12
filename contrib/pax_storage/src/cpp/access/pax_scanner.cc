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
  TableMetadata *meta_info;
  MemoryContext old_ctx;
  FileSystem *file_system;
  MicroPartitionReader *micro_partition_reader;
  TableReader::ReaderOptions reader_options{};

  StaticAssertStmt(
      offsetof(PaxScanDesc, rs_base_) == 0,
      "rs_base should be the first field and aligned to the object address");

  desc = new PaxScanDesc();

  desc->memory_context_ = cbdb::AllocSetCtxCreate(
      CurrentMemoryContext, "Pax Storage", PAX_ALLOCSET_DEFAULT_SIZES);

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

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // build reader
  meta_info = TableMetadata::Create(relation, snapshot);
  file_system = Singleton<LocalFileSystem>::GetInstance();

  micro_partition_reader = new OrcIteratorReader(file_system);
  micro_partition_reader->SetReadBuffer(desc->reused_buffer_);

  reader_options.build_bitmap = true;
  reader_options.rel_oid = desc->rs_base_.rs_rd->rd_id;

  desc->reader_ = new TableReader(micro_partition_reader,
                                  meta_info->NewIterator(), reader_options);
  desc->reader_->Open();

  MemoryContextSwitchTo(old_ctx);
  return &desc->rs_base_;
}

void PaxScanDesc::EndScan(TableScanDesc scan) {
  PaxScanDesc *desc = ToDesc(scan);

  Assert(desc->reader_);
  desc->reader_->Close();
  delete desc->reused_buffer_;
  delete desc->reader_;

  // TODO(jiaqizho): please double check with abort transaction @gongxun
  Assert(desc->memory_context_);
  cbdb::MemoryCtxDelete(desc->memory_context_);
  delete desc;
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(TableScanDesc scan) {
  PaxScanDesc *desc = ToDesc(scan);
  MemoryContext old_ctx;
  Assert(desc && desc->reader_);

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  desc->reader_->ReOpen();
  MemoryContextSwitchTo(old_ctx);
}

bool PaxScanDesc::ScanGetNextSlot(TableScanDesc scan, TupleTableSlot *slot) {
  PaxScanDesc *desc = ToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  CTupleSlot cslot(slot);

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  ok = desc->reader_->ReadTuple(&cslot);
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanAnalyzeNextBlock(TableScanDesc scan,
                                       BlockNumber blockno) {
  PaxScanDesc *desc = ToDesc(scan);
  desc->target_tuple_id_ = blockno;

  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TableScanDesc scan, double *liverows,
                                       const double *deadrows,
                                       TupleTableSlot *slot) {
  PaxScanDesc *desc = ToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  Assert(*deadrows == 0);  // not dead rows in pax latest snapshot

  // skip several tuples if they are not sampling target.
  ok = desc->SeekTuple(desc->target_tuple_id_, &(desc->next_tuple_id_));

  if (!ok) {
    goto finish;
  }

  ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
  desc->next_tuple_id_++;
  if (ok) {
    *liverows += 1;
  }
  // Unlike heap table, latest pax snapshot does not contain deadrows,
  // so `false` value of ok indicate that no more tuple to fetch,
  // and just return directly.

finish:
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextBlock(TableScanDesc scan,
                                      SampleScanState *scanstate) {
  PaxScanDesc *desc = ToDesc(scan);
  MemoryContext old_ctx;
  TsmRoutine *tsm = scanstate->tsmroutine;
  BlockNumber blockno = 0;
  BlockNumber pages = 0;
  double total_tuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  if (desc->total_tuples_ == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(scan->rs_rd, &attrwidths, &pages,
                                           &total_tuples, &allvisfrac);
    desc->total_tuples_ = total_tuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, desc->total_tuples_);
  else
    blockno = system_nextsampleblock(scanstate, desc->total_tuples_);

  ok = BlockNumberIsValid(blockno);
  if (ok) {
    desc->fetch_tuple_id_ = blockno;
  }

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextTuple(TableScanDesc scan,
                                      TupleTableSlot *slot) {
  PaxScanDesc *desc = ToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // skip several tuples if they are not sampling target.
  ok = desc->SeekTuple(desc->fetch_tuple_id_, &desc->next_tuple_id_);

  if (ok) {
    ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
    desc->next_tuple_id_++;
  }

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

uint32 PaxScanDesc::GetMicroPartitionNumber() const {
  return reader_->GetMicroPartitionNumber();
}

uint32 PaxScanDesc::GetCurrentMicroPartitionTupleNumber() const {
  return reader_->GetCurrentMicroPartitionTupleNumber();
}

bool PaxScanDesc::SeekTuple(const uint64 target_tuple_id,
                            uint64 *next_tuple_id) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);
  ok = reader_->SeekTuple(target_tuple_id, next_tuple_id);
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

}  // namespace pax
