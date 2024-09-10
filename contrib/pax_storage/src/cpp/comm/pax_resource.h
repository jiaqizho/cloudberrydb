#pragma once

#include "comm/cbdb_api.h"

namespace pax {
namespace common {

typedef void (*ResourceReleaseFunc)(Datum arg);

void InitResourceCallback();

bool RememberResourceCallback(ResourceReleaseFunc release_func, Datum arg);
bool ForgetResourceCallback(ResourceReleaseFunc release_func, Datum arg);
}
}

namespace paxc {
void ReleaseResourceCallback(ResourceReleasePhase phase, bool is_commit,
                           bool is_top_level, void *arg);
}
