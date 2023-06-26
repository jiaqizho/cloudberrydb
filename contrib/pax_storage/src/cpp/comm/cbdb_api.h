#ifndef SRC_CPP_COMM_CBDB_API_H_
#define SRC_CPP_COMM_CBDB_API_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "postgres.h"  //  NOLINT

#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc.h"
#include "access/tupdesc_details.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "catalog/storage.h"
#include "cdb/cdbvars.h"
#include "common/file_utils.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "postmaster/syslogger.h"  // for PIPE_CHUNK_SIZE
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/syscache.h"

// no header file in cbdb
extern BlockNumber system_nextsampleblock(SampleScanState *node,
                                          BlockNumber nblocks);
#ifdef __cplusplus
}
#endif

#define PAX_TABLE_AM_OID (BITMAP_AM_OID + 1)
#define PAX_AMNAME "pax"
#define PAX_AM_HANDLER_OID 7600
#define PAX_AM_HANDLER_NAME "pax_tableam_handler"

#endif  // SRC_CPP_COMM_CBDB_API_H_
