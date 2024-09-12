#include "storage/oper/pax_stats.h"

#include "comm/cbdb_wrappers.h"

typedef struct {
  const char *opername;
  const StrategyNumber strategy;
} opername_strategy_mapping;

static const opername_strategy_mapping strategy_map[] = {
    {LessStrategyStr, BTLessStrategyNumber},
    {LessEqualStrategyStr, BTLessEqualStrategyNumber},
    {EqualStrategyStr, BTEqualStrategyNumber},
    {GreaterEqualStrategyStr, BTGreaterEqualStrategyNumber},
    {GreaterStrategyStr, BTGreaterStrategyNumber},
};

StrategyNumber OpernameToStrategy(const char *opername) {
  Assert(opername);
  if (!opername) {
    return InvalidStrategy;
  }

  for (size_t i = 0; i < lengthof(strategy_map); i++) {
    if (strcmp(opername, strategy_map[i].opername) == 0) {
      return strategy_map[i].strategy;
    }
  }

  return InvalidStrategy;
}

const char *StrategyToOpername(StrategyNumber number) {
  for (size_t i = 0; i < lengthof(strategy_map); i++) {
    if (strategy_map[i].strategy == number) {
      return strategy_map[i].opername;
    }
  }

  return nullptr;
}

namespace pax {

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum) {
  OperMinMaxKey key;

  if (!CollateIsSupport(collid)) {
    return false;
  }

  key = {atttypid, subtype, strategynum};
  auto it = min_max_opers.find(key);
  if (it != min_max_opers.end()) {
    func = it->second;
    return true;
  }
  return false;
}

bool MinMaxGetPgStrategyProcinfo(Oid atttypid, Oid subtype, FmgrInfo *finfos,
                                 StrategyNumber strategynum) {
  [[maybe_unused]] Oid opno;

  return cbdb::PGGetOperator(StrategyToOpername(strategynum),
                             PG_CATALOG_NAMESPACE, atttypid, subtype, &opno,
                             finfos);
}

bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<FmgrInfo, FmgrInfo> &finfos) {
  Oid opno_less = InvalidOid;
  Oid opno_great = InvalidOid;

  auto ok = cbdb::PGGetOperator(LessStrategyStr, PG_CATALOG_NAMESPACE, typid,
                                subtype, &opno_less, &finfos.first) &&
            cbdb::PGGetOperator(GreaterStrategyStr, PG_CATALOG_NAMESPACE, typid,
                                subtype, &opno_great, &finfos.second);
  AssertImply(ok, OidIsValid(opno_less) && OidIsValid(opno_great));
  return ok;
}

bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs) {
  return MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.first,
                                   BTLessStrategyNumber) &&
         MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.second,
                                   BTGreaterStrategyNumber);
}

bool CollateIsSupport(Oid collid) {
  return collid == InvalidOid ||  // Allow no setting collate
         collid == DEFAULT_COLLATION_OID || collid == C_COLLATION_OID ||
         collid == POSIX_COLLATION_OID;
}

}  // namespace pax
