/*-------------------------------------------------------------------------
 *
 * table.h
 *	  Generic routines for table related code.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/table.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLE_H
#define TABLE_H

#include "nodes/primnodes.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"

extern Relation table_open(Oid relationId, LOCKMODE lockmode);
extern Relation table_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation table_openrv_extended(const RangeVar *relation,
									  LOCKMODE lockmode, bool missing_ok);
extern Relation try_table_open(Oid relationId, LOCKMODE lockmode, bool noWait);
extern void table_close(Relation relation, LOCKMODE lockmode);

/* CDB */
extern Relation CdbOpenTable(Oid relid, LOCKMODE reqmode, bool *lockUpgraded);
extern Relation CdbTryOpenTable(Oid relid, LOCKMODE reqmode,
								bool *lockUpgraded);

/*
 * heap_ used to be the prefix for these routines, and a lot of code will just
 * continue to work without adaptions after the introduction of pluggable
 * storage, therefore just map these names.
 */
#define heap_open(r, l)					table_open(r, l)
#define heap_openrv(r, l)				table_openrv(r, l)
#define heap_openrv_extended(r, l, m)	table_openrv_extended(r, l, m)
#define heap_close(r, l)				table_close(r, l)

#endif							/* TABLE_H */
