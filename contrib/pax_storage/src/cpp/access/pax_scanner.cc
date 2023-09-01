#include "access/pax_scanner.h"

#include "access/pax_access_handle.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_iterator.h"
#include "storage/orc/orc.h"
#include "storage/pax.h"
#include "storage/pax_buffer.h"

namespace pax {

TableScanDesc PaxScanDesc::BeginScan(Relation relation, Snapshot snapshot,
                                     int nkeys, struct ScanKeyData *key,
                                     ParallelTableScanDesc pscan, uint32 flags,
                                     PaxFilter *filter) {
  PaxScanDesc *desc;
  MemoryContext old_ctx;
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
  desc->filter_ = filter;

  // init shared memory
  cbdb::InitCommandResource();

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // build reader
  reader_options.build_bitmap = true;
  reader_options.reused_buffer = desc->reused_buffer_;
  reader_options.rel_oid = desc->rs_base_.rs_rd->rd_id;
  reader_options.filter = filter;

  auto iter = MicroPartitionInfoIterator::New(relation, snapshot);
  if (filter && filter->HasMicroPartitionFilter()) {
    auto wrap = new FilterIterator<MicroPartitionMetadata>(
        std::move(iter), [filter, relation](const auto &x) {
          return filter->TestMicroPartitionScan(x.GetStats(),
                                                RelationGetDescr(relation));
        });
    iter = std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(wrap);
  }
  desc->reader_ = new TableReader(std::move(iter), reader_options);
  desc->reader_->Open();

#ifdef VEC_BUILD
  if (flags & (1 << 12)) {
    desc->vec_adapter_ = new VecAdapter(cbdb::RelationGetTupleDesc(relation));
  }
#endif

  MemoryContextSwitchTo(old_ctx);
  return &desc->rs_base_;
}

void PaxScanDesc::EndScan(TableScanDesc scan) {
  PaxScanDesc *desc = ScanToDesc(scan);

  Assert(desc->reader_);
  desc->reader_->Close();

  delete desc->reused_buffer_;
  delete desc->reader_;
  delete desc->filter_;

#ifdef VEC_BUILD
  delete desc->vec_adapter_;
#endif
  // TODO(jiaqizho): please double check with abort transaction @gongxun
  Assert(desc->memory_context_);
  cbdb::MemoryCtxDelete(desc->memory_context_);
  delete desc;
}

TableScanDesc PaxScanDesc::BeginScanExtractColumns(
    Relation rel, Snapshot snapshot, ParallelTableScanDesc parallel_scan,
    List *targetlist, List *qual, uint32 flags) {
  TableScanDesc paxscan;
  PaxFilter *filter;
  auto natts = cbdb::RelationGetAttributesNumber(rel);
  bool *cols;
  bool found = false;

  filter = new PaxFilter();

  cols = new bool[natts];
  memset(cols, false, natts);

  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(targetlist),
                                       cols, natts);
  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(qual), cols,
                                       natts) ||
          found;

  // In some cases (for example, count(*)), targetlist and qual may be null,
  // extractcolumns_walker will return immediately, so no columns are specified.
  // We always scan the first column.
  if (!found) cols[0] = true;

  // The `cols` life cycle will be bound to `PaxFilter`
  filter->SetColumnProjection(cols, natts);

  {
    ScanKey scan_keys = nullptr;
    int n_scan_keys = 0;
    auto ok = pax::BuildScanKeys(rel, qual, false, &scan_keys, &n_scan_keys);
    if (ok) filter->SetScanKeys(scan_keys, n_scan_keys);
  }
  paxscan = BeginScan(rel, snapshot, 0, nullptr, parallel_scan, flags, filter);

  return paxscan;
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(TableScanDesc scan) {
  PaxScanDesc *desc = ScanToDesc(scan);
  MemoryContext old_ctx;
  Assert(desc && desc->reader_);

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  desc->reader_->ReOpen();
  MemoryContextSwitchTo(old_ctx);
}

bool PaxScanDesc::ScanGetNextSlot(TableScanDesc scan, TupleTableSlot *slot) {
  PaxScanDesc *desc = ScanToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  CTupleSlot cslot(slot);
  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

#ifdef VEC_BUILD
  // TODO(vec): replace (1 << 12), and check other place
  if (desc->rs_base_.rs_flags & (1 << 12)) {
    ok = desc->reader_->ReadVecTuple(&cslot, desc->vec_adapter_);
    MemoryContextSwitchTo(old_ctx);
    return ok;
  }
#endif

  ok = desc->reader_->ReadTuple(&cslot);

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanAnalyzeNextBlock(TableScanDesc scan,
                                       BlockNumber blockno) {
  PaxScanDesc *desc = ScanToDesc(scan);
  desc->target_tuple_id_ = blockno;

  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TableScanDesc scan, double *liverows,
                                       const double *deadrows,
                                       TupleTableSlot *slot) {
  PaxScanDesc *desc = ScanToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  Assert(*deadrows == 0);  // not dead rows in pax latest snapshot
  while (desc->next_tuple_id_ < desc->target_tuple_id_) {
    ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
    if (!ok) break;
    desc->next_tuple_id_++;
  }
  MemoryContextSwitchTo(old_ctx);
  if (ok) *liverows += 1;
  return ok;
}

bool PaxScanDesc::ScanSampleNextBlock(TableScanDesc scan,
                                      SampleScanState *scanstate) {
  PaxScanDesc *desc = ScanToDesc(scan);
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
  PaxScanDesc *desc = ScanToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  while (desc->next_tuple_id_ < desc->fetch_tuple_id_) {
    ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
    if (!ok) break;
    desc->next_tuple_id_++;
  }
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

}  // namespace pax
