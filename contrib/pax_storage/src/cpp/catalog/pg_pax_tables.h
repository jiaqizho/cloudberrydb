#pragma once
#include "comm/cbdb_api.h"

#define NATTS_PG_PAX_TABLES 2
#define ANUM_PG_PAX_TABLES_RELID 1
#define ANUM_PG_PAX_TABLES_AUXRELID 2

namespace paxc {

void InsertPaxTablesEntry(Oid relid, Oid blocksrelid);

void GetPaxTablesEntryAttributes(Oid relid, Oid *blocksrelid);

static inline Oid GetPaxAuxRelid(Oid pax_relid) {
  Oid aux_relid;
  GetPaxTablesEntryAttributes(pax_relid, &aux_relid);
  return aux_relid;
}

}  // namespace paxc
