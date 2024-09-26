#pragma once
#include "comm/cbdb_api.h"

#include <functional>
#include <map>

namespace pax {

using OperMinMaxFunc = std::function<bool(const void *, const void *, Oid)>;
using OperMinMaxKey = std::tuple<Oid, Oid, StrategyNumber>;

extern std::map<OperMinMaxKey, OperMinMaxFunc> min_max_opers;

}  // namespace pax