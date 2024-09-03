#include "storage/micro_partition_iterator.h"

#include "catalog/pax_aux_table.h"
#include "catalog/pg_pax_tables.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {

MicroPartitionInfoIterator::MicroPartitionInfoIterator(Relation pax_rel,
                                                       Snapshot snapshot,
                                                       std::string rel_path)
    : rel_path_(rel_path), pax_rel_(pax_rel), snapshot_(snapshot) {}

MicroPartitionInfoIterator::~MicroPartitionInfoIterator() {
  // FIXME(gongxun): should not release pg resources in destructor function
  End();
}

void MicroPartitionInfoIterator::Begin() {
  Assert(pax_rel_);
  Assert(!desc_);
  Assert(!tuple_);

  if (!aux_rel_) {
    auto aux_oid = paxc::GetPaxAuxRelid(RelationGetRelid(pax_rel_));
    aux_rel_ = table_open(aux_oid, AccessShareLock);
  }

  desc_ = systable_beginscan(aux_rel_, InvalidOid, false, snapshot_, 0, NULL);
}

void MicroPartitionInfoIterator::End() {
  if (desc_) {
    auto desc = desc_;
    auto aux_rel = aux_rel_;
    desc_ = nullptr;
    aux_rel_ = nullptr;
    tuple_ = nullptr;
    systable_endscan(desc);
    table_close(aux_rel, NoLock);
  }
  Assert(!tuple_);
}

bool MicroPartitionInfoIterator::HasNext() {
  if (tuple_) return true;
  tuple_ = cbdb::SystableGetNext(desc_);
  return tuple_ != nullptr;
}

MicroPartitionMetadata MicroPartitionInfoIterator::Next() {
  auto tuple = tuple_;
  Assert(tuple);

  tuple_ = nullptr;
  return std::move(ToValue(tuple));
}

void MicroPartitionInfoIterator::Rewind() {
  CBDB_WRAP_START;
  {
    End();
    Begin();
  }
  CBDB_WRAP_END;
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionInfoIterator::New(Relation pax_rel, Snapshot snapshot) {
  MicroPartitionInfoIterator *it;
  it = PAX_NEW<MicroPartitionInfoIterator>(
      pax_rel, snapshot,
      cbdb::BuildPaxDirectoryPath(
          pax_rel->rd_node, pax_rel->rd_backend,
          cbdb::IsDfsTablespaceById(pax_rel->rd_rel->reltablespace)));
  CBDB_WRAP_FUNCTION(it->Begin);
  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(it);
}

MicroPartitionMetadata MicroPartitionInfoIterator::ToValue(HeapTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  bool is_null;
  auto tup_desc = RelationGetDescr(aux_rel_);

  {
    auto block_id = DatumGetInt32(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    auto file_name =
        cbdb::BuildPaxFilePath(rel_path_, std::to_string(block_id));
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(block_id);
  }

  auto tup_count = cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                                     tup_desc, &is_null);
  CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
  v.SetTupleCount(cbdb::DatumToInt32(tup_count));

  {
    auto stats = reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(
        cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                          tup_desc, &is_null)));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    auto flat_stats = cbdb::PgDeToastDatumPacked(stats);
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(flat_stats),
                                        VARSIZE_ANY_EXHDR(flat_stats));
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError,
               ::pax::fmt("Invalid pb structure in the aux table [rd_id=%d]",
                          aux_rel_->rd_id));
    v.SetStats(std::move(stats_info));

    if (flat_stats != stats) cbdb::Pfree(flat_stats);
  }

  {
    auto visibility_map_filename = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME, tup_desc, &is_null);

    if (!is_null) {
      auto name = NameStr(*DatumGetName(visibility_map_filename));
      auto v_file_name = cbdb::BuildPaxFilePath(rel_path_, name);
      v.SetVisibilityBitmapFile(std::move(v_file_name));
    }
  }

  {
    auto existexttoast = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST, tup_desc, &is_null);
    Assert(!is_null);

    v.SetExistToast(DatumGetBool(existexttoast));

    auto is_cluster = cbdb::DatumToBool(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    v.SetClustered(is_cluster);
  }

  // deserialize protobuf message
  return v;
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionInfoParallelIterator::New(Relation pax_rel, Snapshot snapshot,
                                        ParallelBlockTableScanDesc pscan) {
  MicroPartitionInfoParallelIterator *it;
  it = PAX_NEW<MicroPartitionInfoParallelIterator>(
      pax_rel, snapshot, pscan,
      cbdb::BuildPaxDirectoryPath(
          pax_rel->rd_node, pax_rel->rd_backend,
          cbdb::IsDfsTablespaceById(pax_rel->rd_rel->reltablespace)));
  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(it);
}

MicroPartitionInfoParallelIterator::MicroPartitionInfoParallelIterator(
    Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan,
    std::string rel_path)
    : pax_rel_(pax_rel),
      snapshot_(snapshot),
      pscan_(pscan),
      rel_path_(rel_path) {
  auto aux_oid = cbdb::GetPaxAuxRelid(RelationGetRelid(pax_rel_));
  aux_rel_ = cbdb::TableOpen(aux_oid, AccessShareLock);
  index_oid_ = cbdb::FindAuxIndexOid(aux_rel_->rd_id, snapshot_);

  // init scan context
  CBDB_WRAP_FUNCTION(Begin);
}

MicroPartitionInfoParallelIterator::~MicroPartitionInfoParallelIterator() {
  // close scan context
  CBDB_WRAP_FUNCTION(End);

  cbdb::TableClose(aux_rel_, NoLock);
  aux_rel_ = nullptr;
}

void MicroPartitionInfoParallelIterator::Begin() {
  Assert(pax_rel_);
  Assert(pscan_);
  Assert(!desc_);
  Assert(!tuple_);

  ScanKeyData scan_key[1];
  allocated_block_id_ =
      pg_atomic_fetch_add_u64(&pscan_->phs_nallocated, batch_allocated_);

  ScanKeyInit(&scan_key[0], ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
              BTGreaterEqualStrategyNumber, F_INT4GE, allocated_block_id_);

  desc_ = systable_beginscan(aux_rel_, index_oid_, true, snapshot_, 1,
                             &scan_key[0]);
}

void MicroPartitionInfoParallelIterator::End() {
  if (desc_) {
    auto desc = desc_;
    desc_ = nullptr;
    tuple_ = nullptr;
    systable_endscan(desc);
    // do not reset pscan_, when rewind, we need to keep pscan_ to continue
  }
  Assert(!tuple_);
}

bool MicroPartitionInfoParallelIterator::HasNext() {
  if (tuple_) return true;

  bool is_null;
  CBDB_WRAP_START;
  {
    do {
      tuple_ = systable_getnext(desc_);
      // has no more blocks to scan
      if (tuple_ == nullptr) {
        return false;
      }

      auto block_id = heap_getattr(tuple_, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                                   RelationGetDescr(aux_rel_), &is_null);
      Assert(!is_null);
      // check block_id is in the range of [allocated_block_id_,
      // allocated_block_id_
      // + batch_allocated_)
      if (block_id < allocated_block_id_ + batch_allocated_) {
        return true;
      }

      End();
      // block_id is great than phs_nallocated, update pscan_->phs_nallocated to
      // block_id and switch to next batch
      uint64 old_value = pg_atomic_read_u64(&pscan_->phs_nallocated);
      while (old_value < block_id &&
             !pg_atomic_compare_exchange_u64(&pscan_->phs_nallocated,
                                             &old_value, block_id));
      Begin();
    } while (true);
  }
  CBDB_WRAP_END;

  // has no more blocks to scan
  return false;
}

MicroPartitionMetadata MicroPartitionInfoParallelIterator::Next() {
  auto tuple = tuple_;
  Assert(tuple);

  tuple_ = nullptr;
  return std::move(ToValue(tuple));
}

void MicroPartitionInfoParallelIterator::Rewind() {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements,
             "parallel scan should not call the Rewind function");
}

MicroPartitionMetadata MicroPartitionInfoParallelIterator::ToValue(
    HeapTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  bool is_null;
  auto tup_desc = RelationGetDescr(aux_rel_);

  {
    auto block_id = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, tup_desc, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    std::string block_name = std::to_string(DatumGetInt32(block_id));
    auto file_name = cbdb::BuildPaxFilePath(rel_path_, block_name);
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(DatumGetInt32(block_id));
  }

  auto tup_count = cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                                     tup_desc, &is_null);
  CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
  v.SetTupleCount(cbdb::DatumToInt32(tup_count));

  {
    auto stats = reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(
        cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                          tup_desc, &is_null)));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    auto flat_stats = cbdb::PgDeToastDatumPacked(stats);
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(flat_stats),
                                        VARSIZE_ANY_EXHDR(flat_stats));
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError,
               ::pax::fmt("Invalid pb structure in the aux table [rd_id=%d]",
                          aux_rel_->rd_id));
    v.SetStats(std::move(stats_info));

    if (flat_stats != stats) cbdb::Pfree(flat_stats);
  }

  {
    auto visibility_map_filename = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME, tup_desc, &is_null);

    if (!is_null) {
      auto name = NameStr(*DatumGetName(visibility_map_filename));
      auto v_file_name = cbdb::BuildPaxFilePath(rel_path_, name);
      v.SetVisibilityBitmapFile(std::move(v_file_name));
    }
  }

  {
    auto existexttoast = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST, tup_desc, &is_null);
    Assert(!is_null);

    v.SetExistToast(DatumGetBool(existexttoast));

    auto is_cluster = cbdb::DatumToBool(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    v.SetClustered(is_cluster);
  }

  // deserialize protobuf message
  return v;
}

}  // namespace pax
