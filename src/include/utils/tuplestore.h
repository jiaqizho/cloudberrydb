/*-------------------------------------------------------------------------
 *
 * tuplestore.h
 *	  Generalized routines for temporary tuple storage.
 *
 * This module handles temporary storage of tuples for purposes such
 * as Materialize nodes, hashjoin batch files, etc.  It is essentially
 * a dumbed-down version of tuplesort.c; it does no sorting of tuples
 * but can only store and regurgitate a sequence of tuples.  However,
 * because no sort is required, it is allowed to start reading the sequence
 * before it has all been written.  This is particularly useful for cursors,
 * because it allows random access within the already-scanned portion of
 * a query without having to process the underlying scan to completion.
 * Also, it is possible to support multiple independent read pointers.
 *
 * A temporary file is used to handle the data if it exceeds the
 * space limit specified by the caller.
 *
 * Beginning in Postgres 8.2, what is stored is just MinimalTuples;
 * callers cannot expect valid system columns in regurgitated tuples.
 * Also, we have changed the API to return tuples in TupleTableSlots,
 * so that there is a check to prevent attempted access to system columns.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tuplestore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPLESTORE_H
#define TUPLESTORE_H

#include "executor/tuptable.h"

#include "storage/sharedfileset.h"
#include "utils/resowner.h"

struct Instrumentation;                 /* #include "executor/instrument.h" */

/* Tuplestorestate is an opaque type whose details are not known outside
 * tuplestore.c.
 */
typedef struct Tuplestorestate Tuplestorestate;

extern char * tuplestore_get_buffilename(Tuplestorestate *state);

/*
 * Currently we only need to store MinimalTuples, but it would be easy
 * to support the same behavior for IndexTuples and/or bare Datums.
 */

extern Tuplestorestate *tuplestore_begin_heap(bool randomAccess,
											  bool interXact,
											  int maxKBytes);

extern void tuplestore_set_eflags(Tuplestorestate *state, int eflags);

extern void tuplestore_puttupleslot(Tuplestorestate *state,
									TupleTableSlot *slot);
extern void tuplestore_puttuple(Tuplestorestate *state, HeapTuple tuple);
extern void tuplestore_putvalues(Tuplestorestate *state, TupleDesc tdesc,
								 Datum *values, bool *isnull);

/* tuplestore_donestoring() used to be required, but is no longer used */
#define tuplestore_donestoring(state)	((void) 0)

extern int	tuplestore_alloc_read_pointer(Tuplestorestate *state, int eflags);

extern void tuplestore_select_read_pointer(Tuplestorestate *state, int ptr);

extern void tuplestore_copy_read_pointer(Tuplestorestate *state,
										 int srcptr, int destptr);

extern void tuplestore_trim(Tuplestorestate *state);

extern bool tuplestore_in_memory(Tuplestorestate *state);

extern bool tuplestore_gettupleslot(Tuplestorestate *state, bool forward,
									bool copy, TupleTableSlot *slot);

extern bool tuplestore_advance(Tuplestorestate *state, bool forward);

extern bool tuplestore_skiptuples(Tuplestorestate *state,
								  int64 ntuples, bool forward);

extern int64 tuplestore_tuple_count(Tuplestorestate *state);

extern bool tuplestore_ateof(Tuplestorestate *state);

extern void tuplestore_rescan(Tuplestorestate *state);

extern void tuplestore_clear(Tuplestorestate *state);

extern void tuplestore_end(Tuplestorestate *state);

extern void tuplestore_set_instrument(Tuplestorestate *state,
									  struct Instrumentation *instrument);

extern void tuplestore_make_shared(Tuplestorestate *state, SharedFileSet *fileset,
								   const char *filename);
extern void tuplestore_freeze(Tuplestorestate *state);
extern Tuplestorestate *tuplestore_open_shared(SharedFileSet *fileset, const char *filename);

extern bool tuplestore_has_remaining_tuples(Tuplestorestate *state);

extern void tuplestore_consume_tuple(Tuplestorestate *state);

/*
 * These routines only for IVM.
*/
extern Tuplestorestate *tuplestore_open_shared_noerror(SharedFileSet *fileset, const char *filename);
extern bool tuplestore_in_freezed(Tuplestorestate *state);
extern void tuplestore_set_flags(Tuplestorestate *state, bool isTemp);
extern void tuplestore_make_sharedV2(Tuplestorestate *state, SharedFileSet *fileset,
									const char *filename,
									ResourceOwner owner);
extern void tuplestore_set_tuplecount(Tuplestorestate *state, int64 tuplecount);
extern char *tuplestore_get_sharedname(Tuplestorestate *state);
extern ResourceOwner tuplestore_get_resowner(Tuplestorestate *state);
extern void tuplestore_set_tableid(Tuplestorestate *state, Oid tableid);
extern void tuplestore_set_sharedname(Tuplestorestate *state, char* sharedname);
#endif							/* TUPLESTORE_H */
