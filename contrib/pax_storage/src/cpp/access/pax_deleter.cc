#include "access/pax_deleter.h"

#include <string>
#include <utility>
#include <vector>

#include "access/pax_dml_state.h"
#include "comm/singleton.h"
#include "storage/pax_itemptr.h"
#include "storage/paxc_block_map_manager.h"
namespace pax {
CPaxDeleter::CPaxDeleter(const Relation rel, const Snapshot snapshot)
    : rel_(rel), snapshot_(snapshot) {}

CPaxDeleter::~CPaxDeleter() = default;

TM_Result CPaxDeleter::DeleteTuple(const Relation relation,
                                   const ItemPointer tid, const CommandId cid,
                                   const Snapshot snapshot,
                                   TM_FailureData *tmfd) {
  CPaxDeleter *deleter =
      CPaxDmlStateLocal::Instance()->GetDeleter(relation, snapshot);
  // TODO(gongxun): need more graceful way to pass snapshot
  Assert(deleter != nullptr);
  TM_Result result;
  result = deleter->MarkDelete(tid);
  if (result == TM_SelfModified) {
    tmfd->cmax = cid;
  }
  return result;
}

TM_Result CPaxDeleter::MarkDelete(const ItemPointer tid) {
  PaxItemPointer pax_tid(reinterpret_cast<PaxItemPointer *>(tid));
  uint8 table_no = pax_tid.GetTableNo();
  uint32 block_number = pax_tid.GetBlockNumber();
  uint32 tuple_number = pax_tid.GetTupleNumber();

  std::string block_id =
      cbdb::GetBlockId(rel_->rd_id, table_no, block_number).ToStr();

  if (block_bitmap_map_.find(block_id) == block_bitmap_map_.end()) {
    // TODO(gongxun): bitmap should support dynamic raise size
    block_bitmap_map_[block_id] =
        std::unique_ptr<DynamicBitmap>(new DynamicBitmap());  // NOLINT
  }
  DynamicBitmap *bitmap = block_bitmap_map_[block_id].get();
  if (bitmap->NumBits() <= tuple_number) {
    bitmap->Resize(bitmap->NumBits() * 2);
  }

  if (bitmap->Test(tuple_number)) {
    return TM_SelfModified;
  }

  bitmap->Set(tuple_number);
  return TM_Ok;
}

void CPaxDeleter::ExecDelete() {
  if (block_bitmap_map_.empty()) {
    return;
  }

  TableDeleter table_deleter(rel_, buildDeleteIterator(),
                             std::move(block_bitmap_map_), snapshot_);
  table_deleter.Delete();
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
CPaxDeleter::buildDeleteIterator() {
  std::vector<pax::MicroPartitionMetadata> micro_partitions;
  for (auto &it : block_bitmap_map_) {
    std::string block_id = it.first;
    DynamicBitmap *bitmap_ptr = it.second.get();
    BitmapIterator bitmap_it(bitmap_ptr);
    int32 tuple_number = bitmap_it.Next(true);
    if (tuple_number != -1) {
      pax::MicroPartitionMetadata meta_info;

      meta_info.SetFileName(cbdb::BuildPaxFilePath(rel_, block_id));
      meta_info.SetMicroPartitionId(std::move(block_id));
      micro_partitions.push_back(std::move(meta_info));
    }
  }
  IteratorBase<MicroPartitionMetadata> *iter = new VectorIterator<MicroPartitionMetadata>(std::move(micro_partitions));

  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(iter);
}

}  // namespace pax
