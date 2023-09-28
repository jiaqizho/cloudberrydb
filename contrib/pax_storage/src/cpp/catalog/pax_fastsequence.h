//-------------------------------------------------------------------------
// Cloudberry Database
// Copyright (c) 2023, HashData Technology Limited.
// pax_fastsequence.h
// provide a system table maintaining a light-weight fast sequence number for a unique
// object.
//
// IDENTIFICATION
//	    src/catalog/pax_fastsequence.h
// Author: Tony Ying
//--------------------------------------------------------------------------

#pragma once
#include "comm/cbdb_api.h"

#define ANUM_PG_PAX_FAST_SEQUENCE_OBJID 1
#define ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE 2
#define NATTS_PG_PAX_FAST_SEQUENCE_TABLES 2

#define PG_PAX_FAST_SEQUENCE_NAMESPACE "pg_paxaux"
#define PG_PAX_FAST_SEQUENCE_TABLE "pg_pax_fastsequence"
#define PG_PAX_FAST_SEQUENCE_INDEX_NAME "pg_pax_fastsequence_objid_idx"

namespace paxc {
  void CPaxInsertFastSequenceEntry(Oid objid, int64 last_sequence);

  void CPaxInsertInitialFastSequenceEntries(Oid objid);

  int64 CPaxGetFastSequences(Oid objid);

  int64 CPaxReadLastSequence(Oid objid);

  void CPaxRemoveFastSequenceEntry(Oid objid);
} // namespace

