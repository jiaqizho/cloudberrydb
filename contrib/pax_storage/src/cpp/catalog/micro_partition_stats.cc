#include "catalog/micro_partition_stats.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_metadata.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {
// SetStatsMessage may be called several times in a write,
// one for each micro partition, so all members need to reset.
// Some metainfo like typid, collation, oids for less/greater,
// fmgr should be exactly consistent.
MicroPartitionStats *MicroPartitionStats::SetStatsMessage(
    pax::stats::MicroPartitionStatisticsInfo *stats, int natts) {
  FmgrInfo finfo;
  std::tuple<Oid, Oid, Oid, Oid> zero_oids = {InvalidOid, InvalidOid, InvalidOid, InvalidOid};

  Assert(natts > 0);
  Assert(stats && stats->columnstats_size() == 0);
  initial_check_ = false;
  stats_ = stats;

  memset(&finfo, 0, sizeof(finfo));
  procs_.clear();
  finfos_.clear();
  status_.clear();
  for (int i = 0; i < natts; i++) {
    procs_.emplace_back(zero_oids);
    finfos_.emplace_back(std::pair<FmgrInfo, FmgrInfo>({finfo, finfo}));
    status_.emplace_back('u');
    auto columnstats = stats_->add_columnstats();
    Assert(columnstats->allnull());
    Assert(!columnstats->hasnull());
  }
  Assert(stats_->columnstats_size() == natts);
  return this;
}

void MicroPartitionStats::AddRow(TupleTableSlot *slot) {
  auto desc = slot->tts_tupleDescriptor;
  auto n = desc->natts;

  if (!initial_check_) {
    DoInitialCheck(desc);
    initial_check_ = true;
  }
  CBDB_CHECK(status_.size() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);
  for (auto i = 0; i < n; i++) {
    auto att = &desc->attrs[i];

    AssertImply(att->attisdropped, slot->tts_isnull[i]);
    if (slot->tts_isnull[i])
      AddNullColumn(i);
    else
      AddNonNullColumn(i, slot->tts_values[i], desc);
  }
}

void MicroPartitionStats::AddNullColumn(int column_index) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(procs_.size()));

  auto column_stats = stats_->mutable_columnstats(column_index);
  column_stats->set_hasnull(true);
}

void MicroPartitionStats::AddNonNullColumn(int column_index, Datum value,
                                           TupleDesc desc) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(procs_.size()));

  auto att = TupleDescAttr(desc, column_index);
  auto collation = att->attcollation;
  auto typlen = att->attlen;
  auto typbyval = att->attbyval;
  auto column_stats = stats_->mutable_columnstats(column_index);
  column_stats->set_allnull(false);

  // update min/max
  switch (status_[column_index]) {
    case 'x':
      break;
    case 'y':
      Assert(column_stats->minmaxstats().has_typid());
      Assert(column_stats->minmaxstats().has_minimal());
      Assert(column_stats->minmaxstats().has_maximum());
      Assert(column_stats->minmaxstats().has_proclt());
      Assert(column_stats->minmaxstats().has_procgt());
      Assert(column_stats->minmaxstats().has_procle());
      Assert(column_stats->minmaxstats().has_procge());
      Assert(column_stats->minmaxstats().typid() == att->atttypid);
      Assert(column_stats->minmaxstats().collation() == collation);

      UpdateMinMaxValue(column_index, value, collation, typlen, typbyval);
      break;
    case 'n': {
      auto minmax = column_stats->mutable_minmaxstats();

      Assert(!minmax->has_proclt());
      Assert(!minmax->has_procgt());
      Assert(!minmax->has_procle());
      Assert(!minmax->has_procge());
      Assert(!minmax->has_typid());
      Assert(!minmax->has_minimal());
      Assert(!minmax->has_maximum());

      minmax->set_typid(att->atttypid);
      minmax->set_collation(collation);
      minmax->set_proclt(std::get<0>(procs_[column_index]));
      minmax->set_procgt(std::get<1>(procs_[column_index]));
      minmax->set_procle(std::get<2>(procs_[column_index]));
      minmax->set_procge(std::get<3>(procs_[column_index]));
      minmax->set_minimal(ToValue(value, typlen, typbyval));
      minmax->set_maximum(ToValue(value, typlen, typbyval));
      status_[column_index] = 'y';
      break;
    }
    default:
      Assert(false);
  }
}

void MicroPartitionStats::UpdateMinMaxValue(int column_index, Datum datum,
                                            Oid collation, int typlen,
                                            bool typbyval) {
  Assert(initial_check_);
  Assert(column_index >= 0 && static_cast<size_t>(column_index) < status_.size());
  Assert(status_[column_index] == 'y');

  auto &finfos = finfos_[column_index];
  auto minmax =
      stats_->mutable_columnstats(column_index)->mutable_minmaxstats();
  bool ok;

  {
    const auto &min = minmax->minimal();
    auto val = FromValue(min, typlen, typbyval, &ok);
    CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
    auto update =
        DatumGetBool(cbdb::FunctionCall2Coll(&finfos.first, collation, datum, val));
    if (update) minmax->set_minimal(ToValue(datum, typlen, typbyval));
  }
  {
    const auto &max = minmax->maximum();
    auto val = FromValue(max, typlen, typbyval, &ok);
    CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
    auto update =
        DatumGetBool(cbdb::FunctionCall2Coll(&finfos.second, collation, datum, val));
    if (update) minmax->set_maximum(ToValue(datum, typlen, typbyval));
  }
}

bool MicroPartitionStats::GetStrategyProcinfo(
    Oid typid, std::tuple<Oid, Oid, Oid, Oid> &procids,
    std::pair<FmgrInfo, FmgrInfo> &finfos) {
  return cbdb::MinMaxGetStrategyProcinfo(typid, &std::get<0>(procids), &finfos.first,
                                         BTLessStrategyNumber) &&
         cbdb::MinMaxGetStrategyProcinfo(typid, &std::get<1>(procids), &finfos.second,
                                         BTGreaterStrategyNumber) &&
         cbdb::MinMaxGetStrategyProcinfo(typid, &std::get<2>(procids), nullptr,
                                         BTLessEqualStrategyNumber) &&
         cbdb::MinMaxGetStrategyProcinfo(typid, &std::get<3>(procids), nullptr,
                                         BTGreaterEqualStrategyNumber);
}

void MicroPartitionStats::DoInitialCheck(TupleDesc desc) {
  auto natts = desc->natts;

  Assert(natts == static_cast<int>(status_.size()));
  Assert(natts == stats_->columnstats_size());
  Assert(status_.size() == procs_.size());
  Assert(status_.size() == finfos_.size());

  for (int i = 0; i < natts; i++) {
    auto att = TupleDescAttr(desc, i);
    if (att->attisdropped ||
        !GetStrategyProcinfo(att->atttypid, procs_[i], finfos_[i])) {
      status_[i] = 'x';
      continue;
    }
    status_[i] = 'n';
  }
}

Datum MicroPartitionStats::FromValue(const std::string &s, int typlen,
                                     bool typbyval, bool *ok) {
  const char *p = s.data();
  *ok = true;
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = *reinterpret_cast<const int8 *>(p);
        return cbdb::Int8ToDatum(i);
      }
      case 2: {
        int16 i = *reinterpret_cast<const int16 *>(p);
        return cbdb::Int16ToDatum(i);
      }
      case 4: {
        int32 i = *reinterpret_cast<const int32 *>(p);
        return cbdb::Int32ToDatum(i);
      }
      case 8: {
        int64 i = *reinterpret_cast<const int64 *>(p);
        return cbdb::Int64ToDatum(i);
      }
      default:
        Assert(!"unexpected typbyval, len not in 1,2,4,8");
        *ok = false;
        break;
    }
    return 0;
  }

  Assert(typlen == -1 || typlen > 0);
  return PointerGetDatum(p);
}

std::string MicroPartitionStats::ToValue(Datum datum, int typlen,
                                         bool typbyval) {
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = cbdb::DatumToInt8(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 2: {
        int16 i = cbdb::DatumToInt16(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 4: {
        int32 i = cbdb::DatumToInt32(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 8: {
        int64 i = cbdb::DatumToInt64(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      default:
        Assert(!"unexpected typbyval, len not in 1,2,4,8");
        break;
    }
    CBDB_RAISE(cbdb::CException::kExTypeLogicError);
  }

  if (typlen == -1) {
    void *v;
    int len;

    v = cbdb::PointerAndLenFromDatum(datum, &len);
    Assert(v && len != -1);
    return std::string(reinterpret_cast<char *>(v), len);
  }
  // byref but fixed size
  Assert(typlen > 0);
  return std::string(reinterpret_cast<char *>(cbdb::PointerFromDatum(datum)),
                     typlen);
}
}  // namespace pax

static inline const char *BoolToString(bool b) { return b ? "true" : "false"; }

static char *TypeValueToCString(Oid typid, Oid collation,
                                const std::string &value) {
  FmgrInfo finfo;
  HeapTuple tuple;
  Form_pg_type form;
  Datum datum;
  bool ok;

  tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for type %u", typid);

  form = (Form_pg_type)GETSTRUCT(tuple);
  Assert(OidIsValid(form->typoutput));

  datum = pax::MicroPartitionStats::FromValue(value, form->typlen,
                                              form->typbyval, &ok);
  if (!ok)
    elog(ERROR, "unexpected typlen: %d\n", form->typlen);

  fmgr_info_cxt(form->typoutput, &finfo, CurrentMemoryContext);
  datum = FunctionCall1Coll(&finfo, collation, datum);
  ReleaseSysCache(tuple);

  return DatumGetCString(datum);
}

// define stat type for custom output
extern "C" {
extern Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS);
extern Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(MicroPartitionStatsInput);
PG_FUNCTION_INFO_V1(MicroPartitionStatsOutput);
}

Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS) {
  ereport(ERROR, (errmsg("unsupport MicroPartitionStatsInput")));
  (void)fcinfo;
  PG_RETURN_POINTER(NULL);
}

Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS) {
  struct varlena *v = PG_GETARG_VARLENA_PP(0);
  pax::stats::MicroPartitionStatisticsInfo stats;
  StringInfoData str;

  bool ok = stats.ParseFromArray(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
  if (!ok) ereport(ERROR, (errmsg("micropartition stats is corrupt")));

  initStringInfo(&str);
  for (int i = 0, n = stats.columnstats_size(); i < n; i++) {
    const auto &column = stats.columnstats(i);

    if (i > 0) appendStringInfoChar(&str, ',');

    appendStringInfo(&str, "[(%s,%s)", BoolToString(column.allnull()),
                     BoolToString(column.hasnull()));

    if (!column.has_minmaxstats()) {
      appendStringInfoString(&str, ",None]");
      continue;
    }

    const auto &minmax = column.minmaxstats();
    appendStringInfo(&str, ",(%u,%u,%u,%u,%s,%s)]", minmax.typid(),
                     minmax.collation(), minmax.proclt(),
                     minmax.procgt(),
                     TypeValueToCString(minmax.typid(), minmax.collation(),
                                        minmax.minimal()),
                     TypeValueToCString(minmax.typid(), minmax.collation(),
                                        minmax.maximum()));
  }

  PG_RETURN_CSTRING(str.data);
}
