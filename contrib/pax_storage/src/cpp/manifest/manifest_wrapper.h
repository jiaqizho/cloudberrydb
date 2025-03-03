#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include "postgres.h"
#include "utils/rel.h"

extern bool paxc_need_wal(Relation rel);
extern bool paxc_is_dfs(Oid tablespace);
extern char *paxc_get_pax_dir(RelFileNode rnode, BackendId backend, bool is_dfs);
extern void paxc_create_pax_directory(Relation rel, RelFileNode newrnode, bool is_dfs);
extern void paxc_store_file(const char *filename, const void *data, size_t size);

extern void paxc_read_all(const char *filename, void (*func)(const void *ptr, size_t size, void *opaque), void *opaque);

extern void paxc_wal_insert_if_required(Relation rel, const char *filename, const void *data, size_t size, int64 offset);
extern void paxc_wal_create_directory(RelFileNode node);
extern void paxc_wal_truncate_directory(RelFileNode node);

#ifdef __cplusplus
}
#endif