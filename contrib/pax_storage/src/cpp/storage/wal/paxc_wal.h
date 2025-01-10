#pragma once

#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"
extern "C" {
#endif

#include "storage/wal/paxc_desc.h"


namespace paxc {
void RegisterPaxRmgr();

void XLogPaxInsert(RelFileNode node, const char *filename, int64 offset,
                   void *buffer, int32 bufferLen);

void XLogPaxCreateDirectory(RelFileNode node);

void XLogPaxTruncate(RelFileNode node);

void XLogConsistencyCheck();

void XLogForgetInvalidPaxFile(RelFileNode node, const char *filename);

void XLogForgetRelation(RelFileNode node);

void XLogForgetDatabase(Oid dbId);

}  // namespace paxc

#ifdef __cplusplus
};
#pragma GCC diagnostic pop
#endif
