#pragma once

#include "comm/cbdb_api.h"

namespace cbdb {

void XLogPaxInsert(RelFileNode node, const char *filename, int64 offset,
                   void *buffer, int32 bufferLen);

void XLogPaxCreateDirectory(RelFileNode node);

void XLogPaxTruncate(RelFileNode node);

}  // namespace cbdb