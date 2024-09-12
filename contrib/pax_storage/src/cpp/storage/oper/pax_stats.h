#pragma once
#include "comm/cbdb_api.h"

#include "storage/oper/pax_oper.h"

#define LessStrategyStr "<"
#define LessEqualStrategyStr "<="
#define EqualStrategyStr "="
#define GreaterEqualStrategyStr ">="
#define GreaterStrategyStr ">"

extern StrategyNumber OpernameToStrategy(const char *data);
extern const char *StrategyToOpername(StrategyNumber number);

namespace pax {

// Get the min/max oper from pax
bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum);
// Get the min/max oper from pg
bool MinMaxGetPgStrategyProcinfo(Oid atttypid, Oid subtype, FmgrInfo *finfos,
                                 StrategyNumber strategynum);
// Get the operator from pax
bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs);
// Get the operator from pg
bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<FmgrInfo, FmgrInfo> &finfos);

bool CollateIsSupport(Oid collid);
}  // namespace pax