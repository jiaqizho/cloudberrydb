#include "access/pax_deleter.h"

#include <string>
#include <utility>
#include <vector>

#include "access/pax_dml_state.h"
#include "comm/singleton.h"
#include "storage/pax_itemptr.h"
#include "storage/paxc_block_map_manager.h"
namespace pax {
CPaxDeleter::CPaxDeleter(Relation rel, Snapshot snapshot)
    : rel_(rel), snapshot_(snapshot) {}

CPaxDeleter::~CPaxDeleter() = default;

TM_Result CPaxDeleter::DeleteTuple(Relation relation, ItemPointer tid,
                                   CommandId cid, Snapshot snapshot,
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

TM_Result CPaxDeleter::MarkDelete(ItemPointer tid) {
  PaxItemPointer pax_tid(reinterpret_cast<PaxItemPointer *>(tid));
  uint8 table_no = pax_tid.GetTableNo();
  uint32 block_number = pax_tid.GetBlockNumber();
  uint32 tuple_number = pax_tid.GetTupleNumber();

  std::string block_id =
      cbdb::GetBlockId(rel_->rd_id, table_no, block_number).ToStr();

  if (block_bitmap_map_.find(block_id) == block_bitmap_map_.end()) {
    // TODO(gongxun): bitmap should support dynamic raise size
    block_bitmap_map_[block_id] =
        std::unique_ptr<Bitmap64>(new Bitmap64());  // NOLINT
  }
  auto bitmap = block_bitmap_map_[block_id].get();
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

  TableDeleter table_deleter(rel_, BuildDeleteIterator(),
                             std::move(block_bitmap_map_), snapshot_);
  table_deleter.Delete();
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
CPaxDeleter::BuildDeleteIterator() {
  std::vector<pax::MicroPartitionMetadata> micro_partitions;
  for (auto &it : block_bitmap_map_) {
    std::string block_id = it.first;
    Assert(!it.second->Empty());
    {
      pax::MicroPartitionMetadata meta_info;

      meta_info.SetFileName(cbdb::BuildPaxFilePath(rel_, block_id));
      meta_info.SetMicroPartitionId(std::move(block_id));
      micro_partitions.push_back(std::move(meta_info));
    }
  }
  IteratorBase<MicroPartitionMetadata> *iter =
      new VectorIterator<MicroPartitionMetadata>(std::move(micro_partitions));

  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(iter);
}

}  // namespace pax
