#pragma once
#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
}

class MicroPartitionStats final {
 public:
  MicroPartitionStats() = default;
  MicroPartitionStats *SetStatsMessage(
      pax::stats::MicroPartitionStatisticsInfo *stats, int natts);

  void AddRow(TupleTableSlot *slot);

  static std::string ToValue(Datum datum, int typlen, bool typbyval);
  static Datum FromValue(const std::string &s, int typlen, bool typbyval, bool *ok);

 private:
  void AddNullColumn(int column_index);
  void AddNonNullColumn(int column_index, Datum value, TupleDesc desc);
  void DoInitialCheck(TupleDesc desc);
  void UpdateMinMaxValue(int column_index, Datum datum, Oid collation,
                         int typlen, bool typbyval);
  static bool GetStrategyProcinfo(Oid typid, std::tuple<Oid, Oid, Oid, Oid> &procids,
                                  std::pair<FmgrInfo, FmgrInfo> &finfos);

  // stats_: only references the info object by pointer
  pax::stats::MicroPartitionStatisticsInfo *stats_ = nullptr;

  // less: tuple[0], greater: tuple[1], le: tuple[2], ge: tuple[3]
  std::vector<std::tuple<Oid, Oid, Oid, Oid>> procs_;
  // less: pair[0], greater: pair[1]
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos_;

  // status to indicate whether the oids are initialized
  // or the min-max values are initialized
  // 'u': all is uninitialized
  // 'x': column doesn't support min-max
  // 'n': oids are initialized, but min-max value is missing
  // 'y': min-max is set, needs update.
  std::vector<char> status_;
  bool initial_check_ = false;
};

}  // namespace pax
