#include "access/pax_scanner.h"

#include "access/pax_access_handle.h"
#include "comm/guc.h"
#include "comm/log.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_iterator.h"
#include "storage/orc/orc.h"
#include "storage/pax.h"
#include "storage/pax_buffer.h"

#ifdef ENABLE_PLASMA
#include "storage/cache/pax_plasma_cache.h"
#endif

namespace pax {

TableScanDesc PaxScanDesc::BeginScan(Relation relation, Snapshot snapshot,
                                     int nkeys, struct ScanKeyData *key,
                                     ParallelTableScanDesc pscan, uint32 flags,
                                     PaxFilter *filter, bool build_bitmap) {
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
  if (!desc->filter_) {
    desc->filter_ = new PaxFilter();
  }

  if (!desc->filter_->GetColumnProjection().first) {
    auto natts = cbdb::RelationGetAttributesNumber(relation);
    auto cols = new bool[natts];
    memset(cols, true, natts);
    desc->filter_->SetColumnProjection(cols, natts);
  }

#ifdef VEC_BUILD
  if (flags & (1 << 12)) {
    desc->vec_adapter_ =
        new VecAdapter(cbdb::RelationGetTupleDesc(relation), build_bitmap);
    reader_options.is_vec = true;
    reader_options.adapter = desc->vec_adapter_;
  }
#endif  // VEC_BUILD

#ifdef ENABLE_PLASMA
  if (pax_enable_plasma_in_mem) {
    std::string plasma_socket_path =
        std::string(desc->plasma_socket_path_prefix_);
    plasma_socket_path.append(std::to_string(PostPortNumber));
    plasma_socket_path.append("\0");
    PaxPlasmaCache::CacheOptions cache_options;
    cache_options.domain_socket = plasma_socket_path;
    cache_options.memory_quota = 0;
    cache_options.waitting_ms = 0;

    desc->pax_cache_ = new PaxPlasmaCache(std::move(cache_options));
    auto status = desc->pax_cache_->Initialize();
    if (!status.Ok()) {
      elog(WARNING, "Plasma cache client init failed, message: %s",
           status.Error().c_str());
      delete desc->pax_cache_;
      desc->pax_cache_ = nullptr;
    }

    reader_options.pax_cache = desc->pax_cache_;
  }

#endif  // ENABLE_PLASMA

  // init shared memory
  cbdb::InitCommandResource();

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // build reader
  reader_options.build_bitmap = build_bitmap;
  reader_options.reused_buffer = desc->reused_buffer_;
  reader_options.rel_oid = desc->rs_base_.rs_rd->rd_id;
  reader_options.filter = filter;

  auto iter = MicroPartitionInfoIterator::New(relation, snapshot);
  if (filter && filter->HasMicroPartitionFilter()) {
    auto wrap = new FilterIterator<MicroPartitionMetadata>(
        std::move(iter), [filter, relation](const auto &x) {
          auto ok = filter->TestMicroPartitionScan(x.GetStats(),
                                                   RelationGetDescr(relation));
          PAX_LOG_IF(!ok && pax_enable_debug, "filter micro partition: \"%s\"",
                     x.GetFileName().c_str());
          return ok;
        });
    iter = std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(wrap);
  }
  desc->reader_ = new TableReader(std::move(iter), reader_options);
  desc->reader_->Open();

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

#ifdef ENABLE_PLASMA
  if (desc->pax_cache_) {
    desc->pax_cache_->Destroy();
    delete desc->pax_cache_;
  }
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
  bool build_bitmap = true;
  PaxcExtractcolumnContext extract_column;

  filter = new PaxFilter();

  cols = new bool[natts];
  memset(cols, false, natts);

  extract_column.cols = cols;
  extract_column.natts = natts;

  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(targetlist),
                                       &extract_column);
  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(qual), cols,
                                       natts) ||
          found;
  build_bitmap = cbdb::IsSystemAttrNumExist(&extract_column,
                                            SelfItemPointerAttributeNumber);

  // In some cases (for example, count(*)), targetlist and qual may be null,
  // extractcolumns_walker will return immediately, so no columns are specified.
  // We always scan the first column.
  if (!found && !build_bitmap) cols[0] = true;

  // The `cols` life cycle will be bound to `PaxFilter`
  filter->SetColumnProjection(cols, natts);

  {
    ScanKey scan_keys = nullptr;
    int n_scan_keys = 0;
    auto ok = pax::BuildScanKeys(rel, qual, false, &scan_keys, &n_scan_keys);
    if (ok) filter->SetScanKeys(scan_keys, n_scan_keys);
  }
  paxscan = BeginScan(rel, snapshot, 0, nullptr, parallel_scan, flags, filter,
                      build_bitmap);

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
                                       const double * /* deadrows */,
                                       TupleTableSlot *slot) {
  PaxScanDesc *desc = ScanToDesc(scan);
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);
  while (desc->next_tuple_id_ < desc->target_tuple_id_) {
    ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
    if (!ok) break;
    desc->next_tuple_id_++;
  }
  if (desc->next_tuple_id_ == desc->target_tuple_id_) {
    ok = PaxScanDesc::ScanGetNextSlot(scan, slot);
    desc->next_tuple_id_++;
    if (ok) *liverows += 1;
  }
  MemoryContextSwitchTo(old_ctx);
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
