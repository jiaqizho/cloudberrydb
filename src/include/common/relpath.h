/*-------------------------------------------------------------------------
 *
 * relpath.h
 *		Declarations for GetRelationPath() and friends
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/relpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELPATH_H
#define RELPATH_H

/*
 *	'pgrminclude ignore' needed here because CppAsString2() does not throw
 *	an error if the symbol is not defined.
 */
#include "catalog/catversion.h" /* pgrminclude ignore */


/*
 * Name of major-version-specific tablespace subdirectories
 *
 * In PostgreSQL, this is called just TABLESPACE_VERSION_DIRECTORY..
 * This constant has been renamed so that we catch and know to modify all
 * upstream uses of TABLESPACE_VERSION_DIRECTORY.
 */
#define GP_TABLESPACE_VERSION_DIRECTORY	"GPDB_" GP_MAJORVERSION "_" \
									CppAsString2(CATALOG_VERSION_NO)


/* Characters to allow for an OID in a relation path */
#define OIDCHARS		10		/* max chars printed by %u */

/*
 * Stuff for fork names.
 *
 * The physical storage of a relation consists of one or more forks.
 * The main fork is always created, but in addition to that there can be
 * additional forks for storing various metadata. ForkNumber is used when
 * we need to refer to a specific fork in a relation.
 */
typedef enum ForkNumber
{
	InvalidForkNumber = -1,
	MAIN_FORKNUM = 0,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM,

	/*
	 * Init forks are used to create an initial state that can be used to
	 * quickly revert an object back to its empty state. This is useful for
	 * reverting unlogged tables and indexes back to their initial state during
	 * recovery.
	 */
	INIT_FORKNUM

	/*
	 * NOTE: if you add a new fork, change MAX_FORKNUM and possibly
	 * FORKNAMECHARS below, and update the forkNames array in
	 * src/common/relpath.c
	 */
} ForkNumber;

#define MAX_FORKNUM		INIT_FORKNUM

#define FORKNAMECHARS	4		/* max chars for a fork name */

extern const char *const forkNames[];

extern ForkNumber forkname_to_number(const char *forkName);
extern int	forkname_chars(const char *str, ForkNumber *fork);

/*
 * Stuff for computing filesystem pathnames for relations.
 */
extern char *GetDatabasePath(Oid dbNode, Oid spcNode);

extern char *GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
							 int backendId, ForkNumber forkNumber);

/*
 * Wrapper macros for GetRelationPath.  Beware of multiple
 * evaluation of the RelFileNode or RelFileNodeBackend argument!
 */

/* First argument is a RelFileNode */
#define relpathbackend(rnode, backend, forknum) \
	GetRelationPath((rnode).dbNode, (rnode).spcNode, (rnode).relNode, \
					backend, forknum)

/* First argument is a RelFileNode */
#define relpathperm(rnode, forknum) \
	relpathbackend(rnode, InvalidBackendId, forknum)

/* First argument is a RelFileNodeBackend */
#define relpath(rnode, forknum) \
	relpathbackend((rnode).node, (rnode).backend, forknum)

#define aorelpath(rnode, segno) \
		aorelpathbackend((rnode).node, (rnode).backend, (segno))

#endif							/* RELPATH_H */
