#pragma once

#include "comm/cbdb_api.h"

namespace pax {
void IndexCluster(Relation old_rel, Relation new_rel, Relation index,
                  Snapshot snapshot);
}  // namespace pax
