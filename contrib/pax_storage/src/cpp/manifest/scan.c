#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "utils/rel.h"

#include "catalog/pax_catalog_columns.h"
#include "manifest.h"
#include "tuple.h"

/*
 * start a scan iterator
 */
ManifestScan manifest_beginscan(ManifestRelation mrel, Snapshot snapshot) {
  ManifestScanData *scan;
  MemoryContext oldctx;
  char *manifest_name;

  oldctx = MemoryContextSwitchTo(mrel->mctx);
 
  scan = (ManifestScanData *)palloc(sizeof(ManifestScanData));
  scan->snapshot = snapshot;
  scan->mrel = mrel;
  scan->current_pos = 0;

  manifest_name = get_manifest_top_entrance(mrel->rel, snapshot);
  build_manifest_relation(mrel, manifest_name);

  scan->end_pos = list_length(mrel->heap->manifesttuples);
  MemoryContextSwitchTo(oldctx);

  return scan;
}

void manifest_endscan(ManifestScan scan) {
  pfree(scan);
}

/*
 * support parallel execution, all the manifest tuple memory were allocated
 * when doing open, there will no memory allocating operation in the getnext
 * contextand the position variable was protected by parallel access.
 *
 * currently do not support manifest index scan, for example scan the path
 * stirng by the hash should be much faster, will support it later
 */
ManifestTuple manifest_getnext(ManifestScan scan, void *context) {
  ManifestRelation mrel = scan->mrel;
  ManifestHeap *mheap = mrel->heap;

  while (scan->current_pos < scan->end_pos) {
    ListCell *lc;
    ManifestTuple tuple;

    lc = list_nth_cell(mheap->manifesttuples, scan->current_pos);
    tuple = (ManifestTuple)lfirst(lc);
    scan->current_pos++;

    Assert(!tuple->header.isdeleted);
    return tuple;
  }
  return NULL;
}

/*
 * a fast way to access manifesttuple, by blockid without search from the
 * first to the end  one by one, it take much more time and resource, the
 * best way to implement it is setup a index
 * we consider the manifesttuple has a block field with integer typle, it
 * is the mandatory field.
 *
 * currently we just use a simple seq search to implement the interface and
 * will setup a index later
 */
ManifestTuple manifest_find(ManifestRelation mrel, Snapshot snapshot,
                            int block) {
  ManifestScan scan;
  ManifestTuple mtuple;
  bool isnull;
  
  scan = manifest_beginscan(mrel, snapshot);
  while ((mtuple = manifest_getnext(scan, NULL))) {
    Datum datum;
    int val;

    datum = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTBLOCKNAME, &isnull);
    Assert(!isnull);
    
    val = DatumGetInt32(datum);
    Assert(val >= 0);
    if (val == block)
      break;
  };
  manifest_endscan(scan);
  return mtuple;
}
