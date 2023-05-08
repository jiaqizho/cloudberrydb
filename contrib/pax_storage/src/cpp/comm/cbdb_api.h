#pragma once

extern "C" {
#include "postgres.h"  //  NOLINT

#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pax_tables.h"
#include "executor/tuptable.h"
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/syscache.h"
}
