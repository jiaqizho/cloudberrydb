#pragma once

#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "postgres.h"
#include "storage/relfilenode.h"
#include "access/relation.h"
#include <stdbool.h>
#include <stdint.h>

#include "catalog/manifest_api.h"

#define FLEXIBLE_ARRAY_MEMBER

extern MemoryContext manifest_mcontext;
typedef struct ManifestHeap {
  uint64 version;
  uint64 last_version;
  char *cur_manifest;
  char *last_manifest;
  ManifestDescData *desc;
  bytea *stats;
  List *manifesttuples;
} ManifestHeap;

typedef struct ManifestAttribute {
  const char *field_name;
  MetaFieldType field_type;
  bool has_deflt;
  Datum deflt;
} ManifestAttribute;

#define Get_DESCRIPTOR_ATTRIBUTE_NAME(desc, i) (desc->attrs[i].field_name)

#define Get_DESCRIPTOR_ATTRIBUTE_TYPE(desc, i) (desc->attrs[i].field_type)

typedef struct ManifestRelationData {
  Relation rel; // pax table relation
  MemoryContext mctx;
  ManifestDescData *desc;
  char *paxdir;
  ManifestHeap *heap;
  bool is_dfs;
  bool dirty;
} ManifestRelationData;

typedef struct ManifestTupleHeader {
  bool isdeleted;
  // bits8 t_bits[FLEXIBLE_ARRAY_MEMBER];
  bool *isnulls;
} ManifestTupleHeader;

typedef struct ManifestTupleData {
  ManifestTupleHeader header;
  MetaValue *data;
} ManifestTupleData;

typedef struct ManifestScanData {
  ManifestRelation mrel;
  Snapshot snapshot;
  int current_pos;
  int end_pos;
} ManifestScanData;

typedef ManifestScanData *ManifestScan;

char *get_manifest_top_entrance(Relation rel, Snapshot snapshot);
void build_manifest_relation(ManifestRelation mrel, char *manifest_name);
void build_manifest_heap(const char *paxdir, char *manifest_name, ManifestHeap *mheap);
