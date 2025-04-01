#include "storage/micro_partition_iterator.h"

#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {

MicroPartitionInfoIterator::MicroPartitionInfoIterator(Relation pax_rel,
                                                       Snapshot snapshot,
                                                       std::string rel_path)
    : rel_path_(rel_path), pax_rel_(pax_rel), snapshot_(snapshot) {}

MicroPartitionInfoIterator::~MicroPartitionInfoIterator() { End(); }

void MicroPartitionInfoIterator::Begin() {
  Assert(pax_rel_);
  Assert(!desc_);
  Assert(!tuple_);

  if (!aux_rel_) {
    auto aux_oid = cbdb::GetPaxAuxRelid(RelationGetRelid(pax_rel_));
    aux_rel_ = cbdb::TableOpen(aux_oid, AccessShareLock);
  }

  desc_ =
      cbdb::SystableBeginScan(aux_rel_, InvalidOid, false, snapshot_, 0, NULL);
}

void MicroPartitionInfoIterator::End() {
  if (desc_) {
    auto desc = desc_;
    auto aux_rel = aux_rel_;
    desc_ = nullptr;
    aux_rel_ = nullptr;
    tuple_ = nullptr;
    cbdb::SystableEndScan(desc);
    cbdb::TableClose(aux_rel, NoLock);
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
  End();
  Begin();
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionInfoIterator::New(Relation pax_rel, Snapshot snapshot) {
  MicroPartitionInfoIterator *it;
  it = PAX_NEW<MicroPartitionInfoIterator>(
      pax_rel, snapshot,
      cbdb::BuildPaxDirectoryPath(
          pax_rel->rd_node, pax_rel->rd_backend,
          cbdb::IsDfsTablespaceById(pax_rel->rd_rel->reltablespace)));
  it->Begin();
  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(it);
}

MicroPartitionMetadata MicroPartitionInfoIterator::ToValue(HeapTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  bool is_null;
  auto tup_desc = RelationGetDescr(aux_rel_);

  {
    auto blockid = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, tup_desc, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    auto name = NameStr(*DatumGetName(blockid));
    auto file_name = cbdb::BuildPaxFilePath(rel_path_, name);
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(name);
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
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError);
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

  // deserialize protobuf message
  return v;
}

}  // namespace pax
