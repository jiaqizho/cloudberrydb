#pragma once

#include "comm/cbdb_api.h"

namespace paxc {
#define PAX_MICROPARTITION_NAME_LENGTH 2048
#define PAX_MICROPARTITION_DIR_POSTFIX "_pax"

List* ListDirectory(const char *path);
void CopyFile(const char *srcsegpath, const char *dstsegpath);
void MakedirRecursive(const char *path);
char* BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);
void CreateMicroPartitionFileDirectory(const RelFileNode *rel,
                                       const BackendId rd_backend,
                                       char persistence);
}  // namespace paxc

