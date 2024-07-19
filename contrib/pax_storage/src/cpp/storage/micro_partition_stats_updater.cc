#include "storage/micro_partition_stats_updater.h"

#include "comm/pax_memory.h"
#include "storage/micro_partition_stats.h"
#include "storage/pax_defined.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"

namespace pax {

MicroPartitionStatsUpdater::MicroPartitionStatsUpdater(
    MicroPartitionReader *reader, std::shared_ptr<Bitmap8> visibility_bitmap)
    : reader_(reader) {
  Assert(reader);
  Assert(visibility_bitmap);

  // O(n) here
  size_t group_tup_offset = 0;
  for (size_t i = 0; i < reader_->GetGroupNums(); i++) {
    size_t group_tup_end = reader_->GetTupleCountsInGroup(i) + group_tup_offset;
    bool exist_invisible_tuple = false;
    for (size_t j = group_tup_offset; j < group_tup_end; j++) {
      if (visibility_bitmap->Test(j)) {
        exist_invisible_tuple = true;
        break;
      }
    }
    exist_invisible_tuples_.emplace_back(exist_invisible_tuple);
    group_tup_offset += reader_->GetTupleCountsInGroup(i);
  }
}

MicroPartitionStats *MicroPartitionStatsUpdater::Update(
    TupleTableSlot *slot, const std::vector<int> &minmax_columns) {
  TupleDesc desc;
  MicroPartitionStats *mp_stats, *group_stats = nullptr;

  Assert(slot);
  desc = slot->tts_tupleDescriptor;
  mp_stats = PAX_NEW<MicroPartitionStats>(desc);
  mp_stats->Initialize(minmax_columns);

  Assert(exist_invisible_tuples_.size() == reader_->GetGroupNums());

  for (size_t group_index = 0; group_index < exist_invisible_tuples_.size();
       group_index++) {
    if (exist_invisible_tuples_[group_index]) {
      if (!group_stats) {
        group_stats = PAX_NEW<MicroPartitionStats>(desc);
        group_stats->Initialize(minmax_columns);
      }

      // already setup the visible map
      auto group = reader_->ReadGroup(group_index);
#ifdef ENABLE_DEBUG
      size_t read_count = 0;
#endif
      while (group->ReadTuple(slot).first) {
        group_stats->AddRow(slot, {});
#ifdef ENABLE_DEBUG
        ++read_count;
#endif
      }

#ifdef ENABLE_DEBUG
      // the read counts must less than the tuple counts in group
      Assert(read_count < group->GetRows());
#endif

      PAX_DELETE(group);
    } else {
      ::pax::stats::MicroPartitionStatisticsInfo stat_info;
      auto group_stats = reader_->GetGroupStatsInfo(group_index);
      for (int column_index = 0; column_index < desc->natts; column_index++) {
        auto new_col_stats = stat_info.add_columnstats();

        new_col_stats->mutable_datastats()->CopyFrom(
            group_stats->DataStats(column_index));
        new_col_stats->set_allnull(group_stats->AllNull(column_index));
        new_col_stats->set_hasnull(group_stats->HasNull(column_index));
        new_col_stats->set_nonnullrows(group_stats->NonNullRows(column_index));
        new_col_stats->mutable_info()->CopyFrom(
            group_stats->ColumnInfo(column_index));
      }
      mp_stats->MergeRawInfo(&stat_info);
    }
  }

  if (group_stats) {
    mp_stats->MergeTo(group_stats);
  }

  PAX_DELETE(group_stats);

  return mp_stats;
}

}  // namespace pax
