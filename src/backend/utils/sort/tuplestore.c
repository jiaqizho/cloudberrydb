/*-------------------------------------------------------------------------
 *
 * tuplestore.c
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
 * The (approximate) amount of memory allowed to the tuplestore is specified
 * in kilobytes by the caller.  We absorb tuples and simply store them in an
 * in-memory array as long as we haven't exceeded maxKBytes.  If we do exceed
 * maxKBytes, we dump all the tuples into a temp file and then read from that
 * when needed.
 *
 * Upon creation, a tuplestore supports a single read pointer, numbered 0.
 * Additional read pointers can be created using tuplestore_alloc_read_pointer.
 * Mark/restore behavior is supported by copying read pointers.
 *
 * When the caller requests backward-scan capability, we write the temp file
 * in a format that allows either forward or backward scan.  Otherwise, only
 * forward scan is allowed.  A request for backward scan must be made before
 * putting any tuples into the tuplestore.  Rewind is normally allowed but
 * can be turned off via tuplestore_set_eflags; turning off rewind for all
 * read pointers enables truncation of the tuplestore at the oldest read point
 * for minimal memory usage.  (The caller must explicitly call tuplestore_trim
 * at appropriate times for truncation to actually happen.)
 *
 * Note: in TSS_WRITEFILE state, the temp file's seek position is the
 * current write position, and the write-position variables in the tuplestore
 * aren't kept up to date.  Similarly, in TSS_READFILE state the temp file's
 * seek position is the active read pointer's position, and that read pointer
 * isn't kept up to date.  We update the appropriate variables using ftell()
 * before switching to the other state or activating a different read pointer.
 *
 *
 * Greenplum changes
 * -----------------
 *
 * In Greenplum, tuplestores have one extra capability: a tuplestore can
 * be created and filled in one process, and opened for reading in another
 * process. To do this, call tuplestore_make_shared() immediately
 * after creating the tuplestore, in the writer process. Then populate the
 * tuplestore as usual, by calling tuplestore_puttupleslot(). When you're
 * finished writing to it, call tuplestore_freeze(). tuplestore_freeze()
 * flushes all the tuples to the file. No new rows may be added after
 * freezing it.
 *
 * After freezing, you can open the tupletore for reading in the other
 * process by calling tuplestore_open_shared(). It may be opened for reading
 * as many times as you want, in different processes, until it is destroyed
 * by the original writer process by calling tuplestore_end().
 *
 * Note that tuplestore doesn't do any synchronization across processes!
 * It is up to the calling code to do the freezing, opening for reading, and
 * destroying the tuplestore in the right order!
 *
 * Portions Copyright (c) 2007-2010, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/sort/tuplestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/nodeShareInputScan.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "cdb/cdbvars.h"
#include "executor/instrument.h"        /* struct Instrumentation */
#include "utils/workfile_mgr.h"


/*
 * Possible states of a Tuplestore object.  These denote the states that
 * persist between calls of Tuplestore routines.
 */
typedef enum
{
	TSS_INMEM,					/* Tuples still fit in memory */
	TSS_WRITEFILE,				/* Writing to temp file */
	TSS_READFILE				/* Reading from temp file */
} TupStoreStatus;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
typedef struct
{
	int			eflags;			/* capability flags */
	bool		eof_reached;	/* read has reached EOF */
	int			current;		/* next array index to read */
	int			file;			/* temp file# */
	off_t		offset;			/* byte offset in file */
} TSReadPointer;

typedef enum
{
	TSHARE_NOT_SHARED,
	TSHARE_WRITER,
	TSHARE_READER
} TSSharedStatus;

/*
 * Private state of a Tuplestore operation.
 */
struct Tuplestorestate
{
	TupStoreStatus status;		/* enumerated value as shown above */
	int			eflags;			/* capability flags (OR of pointers' flags) */
	bool		backward;		/* store extra length words in file? */
	bool		interXact;		/* keep open through transactions? */
	bool		truncated;		/* tuplestore_trim has removed tuples? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int64		tuples;			/* number of tuples added */
	BufFile    *myfile;			/* underlying file, or NULL if none */
	MemoryContext context;		/* memory context for holding tuples */
	ResourceOwner resowner;		/* resowner for holding temp files */

	TSSharedStatus share_status;
	bool		frozen;
	SharedFileSet *fileset;
	char	   *shared_filename;
	workfile_set *work_set; /* workfile set to use when using workfile manager */
	int64		remaining_tuples; /* number of tuples remaining */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are handling from the routines that don't need to know it.
	 * They are set up by the tuplestore_begin_xxx routines.
	 *
	 * (Although tuplestore.c currently only supports heap tuples, I've copied
	 * this part of tuplesort.c so that extension to other kinds of objects
	 * will be easy if it's ever needed.)
	 *
	 * Function to copy a supplied input tuple into palloc'd space. (NB: we
	 * assume that a single pfree() is enough to release the tuple later, so
	 * the representation must be "flat" in one palloc chunk.) state->availMem
	 * must be decreased by the amount of space used.
	 */
	void	   *(*copytup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() it, and increase state->availMem by the amount of memory space
	 * thereby released.
	 */
	void		(*writetup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create and return a
	 * palloc'd copy, and decrease state->availMem by the amount of memory
	 * space consumed.
	 */
	void	   *(*readtup) (Tuplestorestate *state, unsigned int len);

	/*
	 * This array holds pointers to tuples in memory if we are in state INMEM.
	 * In states WRITEFILE and READFILE it's not used.
	 *
	 * When memtupdeleted > 0, the first memtupdeleted pointers are already
	 * released due to a tuplestore_trim() operation, but we haven't expended
	 * the effort to slide the remaining pointers down.  These unused pointers
	 * are set to NULL to catch any invalid accesses.  Note that memtupcount
	 * includes the deleted pointers.
	 */
	void	  **memtuples;		/* array of pointers to palloc'd tuples */
	int			memtupdeleted;	/* the first N slots are currently unused */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * These variables are used to keep track of the current positions.
	 *
	 * In state WRITEFILE, the current file seek position is the write point;
	 * in state READFILE, the write position is remembered in writepos_xxx.
	 * (The write position is the same as EOF, but since BufFileSeek doesn't
	 * currently implement SEEK_END, we have to remember it explicitly.)
	 */
	TSReadPointer *readptrs;	/* array of read pointers */
	int			activeptr;		/* index of the active read pointer */
	int			readptrcount;	/* number of pointers currently valid */
	int			readptrsize;	/* allocated length of readptrs array */

	int			writepos_file;	/* file# (valid if READFILE state) */
	off_t		writepos_offset;	/* offset (valid if READFILE state) */

    /*
     * CDB: EXPLAIN ANALYZE reporting interface and statistics.
     */
	struct Instrumentation *instrument;
	long        availMemMin;    /* availMem low water mark (bytes) */
	int64       spilledBytes;   /* memory used for spilled tuples */

	Oid    		tableid;		/* table oid for ivm */
};

#define COPYTUP(state,tup)	((*(state)->copytup) (state, tup))
#define WRITETUP(state,tup) ((*(state)->writetup) (state, tup))
#define READTUP(state,len)	((*(state)->readtup) (state, len))
#define LACKMEM(state)		((state)->availMem < 0)
#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define FREEMEM(state,amt)	\
	do { \
		if ((state)->availMemMin > (state)->availMem) \
			(state)->availMemMin = (state)->availMem; \
		(state)->availMem += (amt); \
	} while(0)

/*--------------------
 *
 * NOTES about on-tape representation of tuples:
 *
 * We require the first "unsigned int" of a stored tuple to be the total size
 * on-tape of the tuple, including itself (so it is never zero).
 * The remainder of the stored tuple
 * may or may not match the in-memory representation of the tuple ---
 * any conversion needed is the job of the writetup and readtup routines.
 *
 * If state->backward is true, then the stored representation of
 * the tuple must be followed by another "unsigned int" that is a copy of the
 * length --- so the total tape space used is actually sizeof(unsigned int)
 * more than the stored length value.  This allows read-backwards.  When
 * state->backward is not set, the write/read routines may omit the extra
 * length word.
 *
 * writetup is expected to write both length words as well as the tuple
 * data.  When readtup is called, the tape is positioned just after the
 * front length word; readtup must read the tuple data and advance past
 * the back length word (if present).
 *
 * The write/read routines can make use of the tuple description data
 * stored in the Tuplestorestate record, if needed. They are also expected
 * to adjust state->availMem by the amount of memory space (not tape space!)
 * released or consumed.  There is no error return from either writetup
 * or readtup; they should ereport() on failure.
 *
 *
 * NOTES about memory consumption calculations:
 *
 * We count space allocated for tuples against the maxKBytes limit,
 * plus the space used by the variable-size array memtuples.
 * Fixed-size space (primarily the BufFile I/O buffer) is not counted.
 * We don't worry about the size of the read pointer array, either.
 *
 * Note that we count actual space used (as shown by GetMemoryChunkSpace)
 * rather than the originally-requested size.  This is important since
 * palloc can add substantial overhead.  It's not a complete answer since
 * we won't count any wasted space in palloc allocation blocks, but it's
 * a lot better than what we were doing before 7.3.
 *
 *--------------------
 */


static Tuplestorestate *tuplestore_begin_common(int eflags,
												bool interXact,
												int maxKBytes);
static void tuplestore_puttuple_common(Tuplestorestate *state, void *tuple);
static void dumptuples(Tuplestorestate *state);
static unsigned int getlen(Tuplestorestate *state, bool eofOK);
static void *copytup_heap(Tuplestorestate *state, void *tup);
static void writetup_heap(Tuplestorestate *state, void *tup);
static void *readtup_heap(Tuplestorestate *state, unsigned int len);


char *
tuplestore_get_buffilename(Tuplestorestate *state)
{
	return state->myfile ? pstrdup(BufFileGetFilename(state->myfile)) : NULL;
}

/*
 *		tuplestore_begin_xxx
 *
 * Initialize for a tuple store operation.
 */
static Tuplestorestate *
tuplestore_begin_common(int eflags, bool interXact, int maxKBytes)
{
	Tuplestorestate *state;

	state = (Tuplestorestate *) palloc0(sizeof(Tuplestorestate));

	state->status = TSS_INMEM;
	state->eflags = eflags;
	state->interXact = interXact;
	state->truncated = false;
	state->allowedMem = maxKBytes * 1024L;
	state->availMem = state->allowedMem;
	state->availMemMin = state->availMem;
	state->myfile = NULL;
	state->context = CurrentMemoryContext;
	state->resowner = CurrentResourceOwner;

	state->memtupdeleted = 0;
	state->memtupcount = 0;
	state->tuples = 0;
	state->remaining_tuples = 0;
	state->tableid = InvalidOid;

	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in grow_memtuples().
	 */
	state->memtupsize = Max(16384 / sizeof(void *),
							ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);

	state->growmemtuples = true;
	state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	state->activeptr = 0;
	state->readptrcount = 1;
	state->readptrsize = 8;		/* arbitrary */
	state->readptrs = (TSReadPointer *)
		palloc(state->readptrsize * sizeof(TSReadPointer));

	state->readptrs[0].eflags = eflags;
	state->readptrs[0].eof_reached = false;
	state->readptrs[0].current = 0;

	return state;
}

/*
 * tuplestore_begin_heap
 *
 * Create a new tuplestore; other types of tuple stores (other than
 * "heap" tuple stores, for heap tuples) are possible, but not presently
 * implemented.
 *
 * randomAccess: if true, both forward and backward accesses to the
 * tuple store are allowed.
 *
 * interXact: if true, the files used for on-disk storage persist beyond the
 * end of the current transaction.  NOTE: It's the caller's responsibility to
 * create such a tuplestore in a memory context and resource owner that will
 * also survive transaction boundaries, and to ensure the tuplestore is closed
 * when it's no longer wanted.
 *
 * maxKBytes: how much data to store in memory (any data beyond this
 * amount is paged to disk).  When in doubt, use work_mem.
 */
Tuplestorestate *
tuplestore_begin_heap(bool randomAccess, bool interXact, int maxKBytes)
{
	Tuplestorestate *state;
	int			eflags;

	/*
	 * This interpretation of the meaning of randomAccess is compatible with
	 * the pre-8.3 behavior of tuplestores.
	 */
	eflags = randomAccess ?
		(EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND) :
		(EXEC_FLAG_REWIND);

	state = tuplestore_begin_common(eflags, interXact, maxKBytes);

	state->copytup = copytup_heap;
	state->writetup = writetup_heap;
	state->readtup = readtup_heap;

	return state;
}

/*
 * tuplestore_set_eflags
 *
 * Set the capability flags for read pointer 0 at a finer grain than is
 * allowed by tuplestore_begin_xxx.  This must be called before inserting
 * any data into the tuplestore.
 *
 * eflags is a bitmask following the meanings used for executor node
 * startup flags (see executor.h).  tuplestore pays attention to these bits:
 *		EXEC_FLAG_REWIND		need rewind to start
 *		EXEC_FLAG_BACKWARD		need backward fetch
 * If tuplestore_set_eflags is not called, REWIND is allowed, and BACKWARD
 * is set per "randomAccess" in the tuplestore_begin_xxx call.
 *
 * NOTE: setting BACKWARD without REWIND means the pointer can read backwards,
 * but not further than the truncation point (the furthest-back read pointer
 * position at the time of the last tuplestore_trim call).
 */
void
tuplestore_set_eflags(Tuplestorestate *state, int eflags)
{
	int			i;

	if (state->status != TSS_INMEM || state->memtupcount != 0)
		elog(ERROR, "too late to call tuplestore_set_eflags");

	state->readptrs[0].eflags = eflags;
	for (i = 1; i < state->readptrcount; i++)
		eflags |= state->readptrs[i].eflags;
	state->eflags = eflags;
}

/*
 * tuplestore_alloc_read_pointer - allocate another read pointer.
 *
 * Returns the pointer's index.
 *
 * The new pointer initially copies the position of read pointer 0.
 * It can have its own eflags, but if any data has been inserted into
 * the tuplestore, these eflags must not represent an increase in
 * requirements.
 */
int
tuplestore_alloc_read_pointer(Tuplestorestate *state, int eflags)
{
	/* Check for possible increase of requirements */
	if (state->status != TSS_INMEM || state->memtupcount != 0)
	{
		if ((state->eflags | eflags) != state->eflags)
			elog(ERROR, "too late to require new tuplestore eflags");
	}

	/* Make room for another read pointer if needed */
	if (state->readptrcount >= state->readptrsize)
	{
		int			newcnt = state->readptrsize * 2;

		state->readptrs = (TSReadPointer *)
			repalloc(state->readptrs, newcnt * sizeof(TSReadPointer));
		state->readptrsize = newcnt;
	}

	/* And set it up */
	state->readptrs[state->readptrcount] = state->readptrs[0];
	state->readptrs[state->readptrcount].eflags = eflags;

	state->eflags |= eflags;

	return state->readptrcount++;
}

/*
 * tuplestore_clear
 *
 *	Delete all the contents of a tuplestore, and reset its read pointers
 *	to the start.
 */
void
tuplestore_clear(Tuplestorestate *state)
{
	int			i;
	TSReadPointer *readptr;

	if (state->myfile)
		BufFileClose(state->myfile);
	state->myfile = NULL;
	if (state->memtuples)
	{
		for (i = state->memtupdeleted; i < state->memtupcount; i++)
		{
			FREEMEM(state, GetMemoryChunkSpace(state->memtuples[i]));
			pfree(state->memtuples[i]);
		}
	}
	state->status = TSS_INMEM;
	state->truncated = false;
	state->memtupdeleted = 0;
	state->memtupcount = 0;
	state->tuples = 0;
	state->remaining_tuples = 0;
	readptr = state->readptrs;
	for (i = 0; i < state->readptrcount; readptr++, i++)
	{
		readptr->eof_reached = false;
		readptr->current = 0;
	}
}

/*
 * tuplestore_end
 *
 *	Release resources and clean up.
 */
void
tuplestore_end(Tuplestorestate *state)
{
	int			i;

	/*
	 * CDB: Report statistics to EXPLAIN ANALYZE.
	 */
	if (state->instrument && state->instrument->need_cdb)
	{
		double  nbytes;

		/* How close did we come to the work_mem limit? */
		FREEMEM(state, 0);              /* update low-water mark */
		nbytes = state->allowedMem - state->availMemMin;
		state->instrument->workmemused = Max(state->instrument->workmemused, nbytes);

		/* How much work_mem would be enough to hold all tuples in memory? */
		if (state->spilledBytes > 0)
		{
			nbytes = state->allowedMem - state->availMem + state->spilledBytes;
			state->instrument->workmemwanted =
				Max(state->instrument->workmemwanted, nbytes);
		}

		if (state->myfile)
			state->instrument->workfileCreated = true;
	}

	if (state->myfile)
		BufFileClose(state->myfile);
	if (state->share_status == TSHARE_WRITER)
		BufFileDeleteShared(state->fileset, state->shared_filename);
	if (state->work_set)
		workfile_mgr_close_set(state->work_set);
	if (state->shared_filename)
		pfree(state->shared_filename);
	if (state->memtuples)
	{
		for (i = state->memtupdeleted; i < state->memtupcount; i++)
			pfree(state->memtuples[i]);
		pfree(state->memtuples);
	}
	pfree(state->readptrs);
	pfree(state);
}

/*
 * tuplestore_select_read_pointer - make the specified read pointer active
 */
void
tuplestore_select_read_pointer(Tuplestorestate *state, int ptr)
{
	TSReadPointer *readptr;
	TSReadPointer *oldptr;

	Assert(ptr >= 0 && ptr < state->readptrcount);

	/* No work if already active */
	if (ptr == state->activeptr)
		return;

	readptr = &state->readptrs[ptr];
	oldptr = &state->readptrs[state->activeptr];

	switch (state->status)
	{
		case TSS_INMEM:
		case TSS_WRITEFILE:
			/* no work */
			break;
		case TSS_READFILE:

			/*
			 * First, save the current read position in the pointer about to
			 * become inactive.
			 */
			if (!oldptr->eof_reached)
				BufFileTell(state->myfile,
							&oldptr->file,
							&oldptr->offset);

			/*
			 * We have to make the temp file's seek position equal to the
			 * logical position of the new read pointer.  In eof_reached
			 * state, that's the EOF, which we have available from the saved
			 * write position.
			 */
			if (readptr->eof_reached)
			{
				if (BufFileSeek(state->myfile,
								state->writepos_file,
								state->writepos_offset,
								SEEK_SET) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek in tuplestore temporary file")));
			}
			else
			{
				if (BufFileSeek(state->myfile,
								readptr->file,
								readptr->offset,
								SEEK_SET) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek in tuplestore temporary file")));
			}
			break;
		default:
			elog(ERROR, "invalid tuplestore state");
			break;
	}

	state->activeptr = ptr;
}

/*
 * tuplestore_tuple_count
 *
 * Returns the number of tuples added since creation or the last
 * tuplestore_clear().
 */
int64
tuplestore_tuple_count(Tuplestorestate *state)
{
	return state->tuples;
}

/*
 * tuplestore_ateof
 *
 * Returns the active read pointer's eof_reached state.
 */
bool
tuplestore_ateof(Tuplestorestate *state)
{
	return state->readptrs[state->activeptr].eof_reached;
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
static bool
grow_memtuples(Tuplestorestate *state)
{
	int			newmemtupsize;
	int			memtupsize = state->memtupsize;
	int64		memNowUsed = state->allowedMem - state->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!state->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= state->availMem)
	{
		/*
		 * We've used no more than half of allowedMem; double our usage,
		 * clamping at INT_MAX tuples.
		 */
		if (memtupsize < INT_MAX / 2)
			newmemtupsize = memtupsize * 2;
		else
		{
			newmemtupsize = INT_MAX;
			state->growmemtuples = false;
		}
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(void *) elements. In practice, we want to
		 * increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.  We again
		 * clamp at INT_MAX tuples.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  Any inaccuracy in the
		 * result should be insignificant; but even if we computed a
		 * completely insane result, the checks below will prevent anything
		 * really bad from happening.
		 */
		double		grow_ratio;

		grow_ratio = (double) state->allowedMem / (double) memNowUsed;
		if (memtupsize * grow_ratio < INT_MAX)
			newmemtupsize = (int) (memtupsize * grow_ratio);
		else
			newmemtupsize = INT_MAX;

		/* We won't make any further enlargement attempts */
		state->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
	 * to ensure our request won't be rejected.  Note that we can easily
	 * exhaust address space before facing this outcome.  (This is presently
	 * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
	 * don't rely on that at this distance.)
	 */
	if ((Size) newmemtupsize >= MaxAllocHugeSize / sizeof(void *))
	{
		newmemtupsize = (int) (MaxAllocHugeSize / sizeof(void *));
		state->growmemtuples = false;	/* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (state->availMem < (int64) ((newmemtupsize - memtupsize) * sizeof(void *)))
		goto noalloc;

	/* OK, do it */
	FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;
	state->memtuples = (void **)
		repalloc_huge(state->memtuples,
					  state->memtupsize * sizeof(void *));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
	if (LACKMEM(state))
		elog(ERROR, "unexpected out-of-memory situation in tuplestore");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	state->growmemtuples = false;
	return false;
}

/*
 * Accept one tuple and append it to the tuplestore.
 *
 * Note that the input tuple is always copied; the caller need not save it.
 *
 * If the active read pointer is currently "at EOF", it remains so (the read
 * pointer implicitly advances along with the write pointer); otherwise the
 * read pointer is unchanged.  Non-active read pointers do not move, which
 * means they are certain to not be "at EOF" immediately after puttuple.
 * This curious-seeming behavior is for the convenience of nodeMaterial.c and
 * nodeCtescan.c, which would otherwise need to do extra pointer repositioning
 * steps.
 *
 * tuplestore_puttupleslot() is a convenience routine to collect data from
 * a TupleTableSlot without an extra copy operation.
 */
void
tuplestore_puttupleslot(Tuplestorestate *state,
						TupleTableSlot *slot)
{
	MinimalTuple tuple;
	MemoryContext oldcxt = MemoryContextSwitchTo(state->context);

	/*
	 * Form a MinimalTuple in working memory
	 */
	tuple = ExecCopySlotMinimalTuple(slot);
	USEMEM(state, GetMemoryChunkSpace(tuple));

	tuplestore_puttuple_common(state, (void *) tuple);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * "Standard" case to copy from a HeapTuple.  This is actually now somewhat
 * deprecated, but not worth getting rid of in view of the number of callers.
 */
void
tuplestore_puttuple(Tuplestorestate *state, HeapTuple tuple)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(state->context);

	/*
	 * Copy the tuple.  (Must do this even in WRITEFILE case.  Note that
	 * COPYTUP includes USEMEM, so we needn't do that here.)
	 */
	tuple = COPYTUP(state, tuple);

	tuplestore_puttuple_common(state, (void *) tuple);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Similar to tuplestore_puttuple(), but work from values + nulls arrays.
 * This avoids an extra tuple-construction operation.
 */
void
tuplestore_putvalues(Tuplestorestate *state, TupleDesc tdesc,
					 Datum *values, bool *isnull)
{
	MinimalTuple tuple;
	MemoryContext oldcxt = MemoryContextSwitchTo(state->context);

	tuple = heap_form_minimal_tuple(tdesc, values, isnull);

	USEMEM(state, GetMemoryChunkSpace(tuple));

	tuplestore_puttuple_common(state, (void *) tuple);

	MemoryContextSwitchTo(oldcxt);
}

static void
tuplestore_puttuple_common(Tuplestorestate *state, void *tuple)
{
	TSReadPointer *readptr;
	int			i;
	ResourceOwner oldowner;

	if (state->frozen)
		elog(ERROR, "cannot write new tuples to frozen tuplestore");

	state->tuples++;
	state->remaining_tuples++;

	switch (state->status)
	{
		case TSS_INMEM:

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->current = state->memtupcount;
				}
			}

			/*
			 * Grow the array as needed.  Note that we try to grow the array
			 * when there is still one free slot remaining --- if we fail,
			 * there'll still be room to store the incoming tuple, and then
			 * we'll switch to tape-based operation.
			 */
			if (state->memtupcount >= state->memtupsize - 1)
			{
				(void) grow_memtuples(state);
				Assert(state->memtupcount < state->memtupsize);
			}

			/* Stash the tuple in the in-memory array */
			state->memtuples[state->memtupcount++] = tuple;

			/*
			 * Done if we still fit in available memory and have array slots.
			 */
			if (state->memtupcount < state->memtupsize && !LACKMEM(state))
				return;

			if (OidIsValid(state->tableid) && state->shared_filename)
			{
				tuplestore_make_sharedV2(state,
							get_shareinput_fileset(),
							state->shared_filename,
							tuplestore_get_resowner(state));
			}
			else
			{
				/*
				* Nope; time to switch to tape-based operation.  Make sure that
				* the temp file(s) are created in suitable temp tablespaces.
				*/
				PrepareTempTablespaces();

				/* associate the file with the store's resource owner */
				oldowner = CurrentResourceOwner;
				CurrentResourceOwner = state->resowner;

				char tmpprefix[50];
				snprintf(tmpprefix, 50, "slice%d_tuplestore", currentSliceId);
				state->myfile = BufFileCreateTemp(tmpprefix, state->interXact);

				CurrentResourceOwner = oldowner;

				/*
				* Freeze the decision about whether trailing length words will be
				* used.  We can't change this choice once data is on tape, even
				* though callers might drop the requirement.
				*/
				state->backward = (state->eflags & EXEC_FLAG_BACKWARD) != 0;
				state->status = TSS_WRITEFILE;
			}
			dumptuples(state);
			break;
		case TSS_WRITEFILE:

			/*
			 * Update read pointers as needed; see API spec above. Note:
			 * BufFileTell is quite cheap, so not worth trying to avoid
			 * multiple calls.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					BufFileTell(state->myfile,
								&readptr->file,
								&readptr->offset);
				}
			}

			WRITETUP(state, tuple);
			break;
		case TSS_READFILE:

			/*
			 * Switch from reading to writing.
			 */
			if (!state->readptrs[state->activeptr].eof_reached)
				BufFileTell(state->myfile,
							&state->readptrs[state->activeptr].file,
							&state->readptrs[state->activeptr].offset);
			if (BufFileSeek(state->myfile,
							state->writepos_file, state->writepos_offset,
							SEEK_SET) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in tuplestore temporary file")));
			state->status = TSS_WRITEFILE;

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->file = state->writepos_file;
					readptr->offset = state->writepos_offset;
				}
			}

			WRITETUP(state, tuple);
			break;
		default:
			elog(ERROR, "invalid tuplestore state");
			break;
	}
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to tuplestore_set_eflags().
 */
static void *
tuplestore_gettuple(Tuplestorestate *state, bool forward,
					bool *should_free)
{
	TSReadPointer *readptr = &state->readptrs[state->activeptr];
	unsigned int tuplen;
	void	   *tup;

	Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

	switch (state->status)
	{
		case TSS_INMEM:
			*should_free = false;
			if (forward)
			{
				if (readptr->eof_reached)
					return NULL;
				if (readptr->current < state->memtupcount)
				{
					/* We have another tuple, so return it */
					return state->memtuples[readptr->current++];
				}
				readptr->eof_reached = true;
				return NULL;
			}
			else
			{
				/*
				 * if all tuples are fetched already then we return last
				 * tuple, else tuple before last returned.
				 */
				if (readptr->eof_reached)
				{
					readptr->current = state->memtupcount;
					readptr->eof_reached = false;
				}
				else
				{
					if (readptr->current <= state->memtupdeleted)
					{
						Assert(!state->truncated);
						return NULL;
					}
					readptr->current--; /* last returned tuple */
				}
				if (readptr->current <= state->memtupdeleted)
				{
					Assert(!state->truncated);
					return NULL;
				}
				return state->memtuples[readptr->current - 1];
			}
			break;

		case TSS_WRITEFILE:
			/* Skip state change if we'll just return NULL */
			if (readptr->eof_reached && forward)
				return NULL;

			/*
			 * Switch from writing to reading.
			 */
			BufFileTell(state->myfile,
						&state->writepos_file, &state->writepos_offset);
			if (!readptr->eof_reached)
				if (BufFileSeek(state->myfile,
								readptr->file, readptr->offset,
								SEEK_SET) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek in tuplestore temporary file")));
			state->status = TSS_READFILE;
			/* FALLTHROUGH */

		case TSS_READFILE:
			*should_free = true;
			if (forward)
			{
				if ((tuplen = getlen(state, true)) != 0)
				{
					tup = READTUP(state, tuplen);

					/* CDB XXX XXX XXX XXX */
					/* MPP-1347: EXPLAIN ANALYZE shows runaway memory usage.
					 * Readtup does a usemem, but the free happens in
					 * ExecStoreTuple.  Do a free so state->availMem
					 * doesn't go massively negative to screw up
					 * stats.  It would be better to interrogate the
					 * heap for actual memory usage than use this
					 * homemade accounting.
					 */
					FREEMEM(state, GetMemoryChunkSpace(tup));
					/* CDB XXX XXX XXX XXX */
					return tup;
				}
				else
				{
					readptr->eof_reached = true;
					return NULL;
				}
			}

			/*
			 * Backward.
			 *
			 * if all tuples are fetched already then we return last tuple,
			 * else tuple before last returned.
			 *
			 * Back up to fetch previously-returned tuple's ending length
			 * word. If seek fails, assume we are at start of file.
			 */
			if (BufFileSeek(state->myfile, 0, -(long) sizeof(unsigned int),
							SEEK_CUR) != 0)
			{
				/* even a failed backwards fetch gets you out of eof state */
				readptr->eof_reached = false;
				Assert(!state->truncated);
				return NULL;
			}
			tuplen = getlen(state, false);

			if (readptr->eof_reached)
			{
				readptr->eof_reached = false;
				/* We will return the tuple returned before returning NULL */
			}
			else
			{
				/*
				 * Back up to get ending length word of tuple before it.
				 */
				if (BufFileSeek(state->myfile, 0,
								-(long) (tuplen + 2 * sizeof(unsigned int)),
								SEEK_CUR) != 0)
				{
					/*
					 * If that fails, presumably the prev tuple is the first
					 * in the file.  Back up so that it becomes next to read
					 * in forward direction (not obviously right, but that is
					 * what in-memory case does).
					 */
					if (BufFileSeek(state->myfile, 0,
									-(long) (tuplen + sizeof(unsigned int)),
									SEEK_CUR) != 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not seek in tuplestore temporary file")));
					Assert(!state->truncated);
					return NULL;
				}
				tuplen = getlen(state, false);
			}

			/*
			 * Now we have the length of the prior tuple, back up and read it.
			 * Note: READTUP expects we are positioned after the initial
			 * length word of the tuple, so back up to that point.
			 */
			if (BufFileSeek(state->myfile, 0,
							-(long) tuplen,
							SEEK_CUR) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in tuplestore temporary file")));
			tup = READTUP(state, tuplen);
			return tup;

		default:
			elog(ERROR, "invalid tuplestore state");
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * tuplestore_gettupleslot - exported function to fetch a MinimalTuple
 *
 * If successful, put tuple in slot and return true; else, clear the slot
 * and return false.
 *
 * If copy is true, the slot receives a copied tuple (allocated in current
 * memory context) that will stay valid regardless of future manipulations of
 * the tuplestore's state.  If copy is false, the slot may just receive a
 * pointer to a tuple held within the tuplestore.  The latter is more
 * efficient but the slot contents may be corrupted if additional writes to
 * the tuplestore occur.  (If using tuplestore_trim, see comments therein.)
 */
bool
tuplestore_gettupleslot(Tuplestorestate *state, bool forward,
						bool copy, TupleTableSlot *slot)
{
	MinimalTuple tuple;
	bool		should_free;

	tuple = (MinimalTuple) tuplestore_gettuple(state, forward, &should_free);

	if (tuple)
	{
		if (copy && !should_free)
		{
			tuple = heap_copy_minimal_tuple(tuple);
			should_free = true;
		}
		ExecStoreMinimalTuple(tuple, slot, should_free);
		return true;
	}
	else
	{
		ExecClearTuple(slot);
		return false;
	}
}

/*
 * tuplestore_advance - exported function to adjust position without fetching
 *
 * We could optimize this case to avoid palloc/pfree overhead, but for the
 * moment it doesn't seem worthwhile.
 */
bool
tuplestore_advance(Tuplestorestate *state, bool forward)
{
	void	   *tuple;
	bool		should_free;

	tuple = tuplestore_gettuple(state, forward, &should_free);

	if (tuple)
	{
		if (should_free)
			pfree(tuple);
		return true;
	}
	else
	{
		return false;
	}
}

/*
 * Advance over N tuples in either forward or back direction,
 * without returning any data.  N<=0 is a no-op.
 * Returns true if successful, false if ran out of tuples.
 */
bool
tuplestore_skiptuples(Tuplestorestate *state, int64 ntuples, bool forward)
{
	TSReadPointer *readptr = &state->readptrs[state->activeptr];

	Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

	if (ntuples <= 0)
		return true;

	switch (state->status)
	{
		case TSS_INMEM:
			if (forward)
			{
				if (readptr->eof_reached)
					return false;
				if (state->memtupcount - readptr->current >= ntuples)
				{
					readptr->current += ntuples;
					return true;
				}
				readptr->current = state->memtupcount;
				readptr->eof_reached = true;
				return false;
			}
			else
			{
				if (readptr->eof_reached)
				{
					readptr->current = state->memtupcount;
					readptr->eof_reached = false;
					ntuples--;
				}
				if (readptr->current - state->memtupdeleted > ntuples)
				{
					readptr->current -= ntuples;
					return true;
				}
				Assert(!state->truncated);
				readptr->current = state->memtupdeleted;
				return false;
			}
			break;

		default:
			/* We don't currently try hard to optimize other cases */
			while (ntuples-- > 0)
			{
				void	   *tuple;
				bool		should_free;

				tuple = tuplestore_gettuple(state, forward, &should_free);

				if (tuple == NULL)
					return false;
				if (should_free)
					pfree(tuple);
				CHECK_FOR_INTERRUPTS();
			}
			return true;
	}
}

/*
 * dumptuples - remove tuples from memory and write to tape
 *
 * As a side effect, we must convert each read pointer's position from
 * "current" to file/offset format.  But eof_reached pointers don't
 * need to change state.
 */
static void
dumptuples(Tuplestorestate *state)
{
	int			i;

	for (i = state->memtupdeleted;; i++)
	{
		TSReadPointer *readptr = state->readptrs;
		int			j;

		for (j = 0; j < state->readptrcount; readptr++, j++)
		{
			if (i == readptr->current && !readptr->eof_reached)
				BufFileTell(state->myfile,
							&readptr->file, &readptr->offset);
		}
		if (i >= state->memtupcount)
			break;
		WRITETUP(state, state->memtuples[i]);
	}
	state->memtupdeleted = 0;
	state->memtupcount = 0;
}

/*
 * tuplestore_rescan		- rewind the active read pointer to start
 */
void
tuplestore_rescan(Tuplestorestate *state)
{
	TSReadPointer *readptr = &state->readptrs[state->activeptr];

	Assert(readptr->eflags & EXEC_FLAG_REWIND);
	Assert(!state->truncated);

	switch (state->status)
	{
		case TSS_INMEM:
			readptr->eof_reached = false;
			readptr->current = 0;
			break;
		case TSS_WRITEFILE:
			readptr->eof_reached = false;
			readptr->file = 0;
			readptr->offset = 0L;
			break;
		case TSS_READFILE:
			readptr->eof_reached = false;
			if (BufFileSeek(state->myfile, 0, 0L, SEEK_SET) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in tuplestore temporary file")));
			break;
		default:
			elog(ERROR, "invalid tuplestore state");
			break;
	}
}

/*
 * tuplestore_copy_read_pointer - copy a read pointer's state to another
 */
void
tuplestore_copy_read_pointer(Tuplestorestate *state,
							 int srcptr, int destptr)
{
	TSReadPointer *sptr = &state->readptrs[srcptr];
	TSReadPointer *dptr = &state->readptrs[destptr];

	Assert(srcptr >= 0 && srcptr < state->readptrcount);
	Assert(destptr >= 0 && destptr < state->readptrcount);

	/* Assigning to self is a no-op */
	if (srcptr == destptr)
		return;

	if (dptr->eflags != sptr->eflags)
	{
		/* Possible change of overall eflags, so copy and then recompute */
		int			eflags;
		int			i;

		*dptr = *sptr;
		eflags = state->readptrs[0].eflags;
		for (i = 1; i < state->readptrcount; i++)
			eflags |= state->readptrs[i].eflags;
		state->eflags = eflags;
	}
	else
		*dptr = *sptr;

	switch (state->status)
	{
		case TSS_INMEM:
		case TSS_WRITEFILE:
			/* no work */
			break;
		case TSS_READFILE:

			/*
			 * This case is a bit tricky since the active read pointer's
			 * position corresponds to the seek point, not what is in its
			 * variables.  Assigning to the active requires a seek, and
			 * assigning from the active requires a tell, except when
			 * eof_reached.
			 */
			if (destptr == state->activeptr)
			{
				if (dptr->eof_reached)
				{
					if (BufFileSeek(state->myfile,
									state->writepos_file,
									state->writepos_offset,
									SEEK_SET) != 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not seek in tuplestore temporary file")));
				}
				else
				{
					if (BufFileSeek(state->myfile,
									dptr->file, dptr->offset,
									SEEK_SET) != 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not seek in tuplestore temporary file")));
				}
			}
			else if (srcptr == state->activeptr)
			{
				if (!dptr->eof_reached)
					BufFileTell(state->myfile,
								&dptr->file,
								&dptr->offset);
			}
			break;
		default:
			elog(ERROR, "invalid tuplestore state");
			break;
	}
}

/*
 * tuplestore_trim	- remove all no-longer-needed tuples
 *
 * Calling this function authorizes the tuplestore to delete all tuples
 * before the oldest read pointer, if no read pointer is marked as requiring
 * REWIND capability.
 *
 * Note: this is obviously safe if no pointer has BACKWARD capability either.
 * If a pointer is marked as BACKWARD but not REWIND capable, it means that
 * the pointer can be moved backward but not before the oldest other read
 * pointer.
 */
void
tuplestore_trim(Tuplestorestate *state)
{
	int			oldest;
	int			nremove;
	int			i;

	/*
	 * Truncation is disallowed if any read pointer requires rewind
	 * capability.
	 */
	if (state->eflags & EXEC_FLAG_REWIND)
		return;

	/* Cannot trim tuplestore if another process might be reading it */
	if (state->frozen)
		return;

	/*
	 * We don't bother trimming temp files since it usually would mean more
	 * work than just letting them sit in kernel buffers until they age out.
	 */
	if (state->status != TSS_INMEM)
		return;

	/* Find the oldest read pointer */
	oldest = state->memtupcount;
	for (i = 0; i < state->readptrcount; i++)
	{
		if (!state->readptrs[i].eof_reached)
			oldest = Min(oldest, state->readptrs[i].current);
	}

	/*
	 * Note: you might think we could remove all the tuples before the oldest
	 * "current", since that one is the next to be returned.  However, since
	 * tuplestore_gettuple returns a direct pointer to our internal copy of
	 * the tuple, it's likely that the caller has still got the tuple just
	 * before "current" referenced in a slot. So we keep one extra tuple
	 * before the oldest "current".  (Strictly speaking, we could require such
	 * callers to use the "copy" flag to tuplestore_gettupleslot, but for
	 * efficiency we allow this one case to not use "copy".)
	 */
	nremove = oldest - 1;
	if (nremove <= 0)
		return;					/* nothing to do */

	Assert(nremove >= state->memtupdeleted);
	Assert(nremove <= state->memtupcount);

	/* Release no-longer-needed tuples */
	for (i = state->memtupdeleted; i < nremove; i++)
	{
		FREEMEM(state, GetMemoryChunkSpace(state->memtuples[i]));
		pfree(state->memtuples[i]);
		state->memtuples[i] = NULL;
	}
	state->memtupdeleted = nremove;

	/* mark tuplestore as truncated (used for Assert crosschecks only) */
	state->truncated = true;

	/*
	 * If nremove is less than 1/8th memtupcount, just stop here, leaving the
	 * "deleted" slots as NULL.  This prevents us from expending O(N^2) time
	 * repeatedly memmove-ing a large pointer array.  The worst case space
	 * wastage is pretty small, since it's just pointers and not whole tuples.
	 */
	if (nremove < state->memtupcount / 8)
		return;

	/*
	 * Slide the array down and readjust pointers.
	 *
	 * In mergejoin's current usage, it's demonstrable that there will always
	 * be exactly one non-removed tuple; so optimize that case.
	 */
	if (nremove + 1 == state->memtupcount)
		state->memtuples[0] = state->memtuples[nremove];
	else
		memmove(state->memtuples, state->memtuples + nremove,
				(state->memtupcount - nremove) * sizeof(void *));

	state->memtupdeleted = 0;
	state->memtupcount -= nremove;
	for (i = 0; i < state->readptrcount; i++)
	{
		if (!state->readptrs[i].eof_reached)
			state->readptrs[i].current -= nremove;
	}
}

/*
 * tuplestore_in_memory
 *
 * Returns true if the tuplestore has not spilled to disk.
 *
 * XXX exposing this is a violation of modularity ... should get rid of it.
 */
bool
tuplestore_in_memory(Tuplestorestate *state)
{
	return (state->status == TSS_INMEM);
}


/*
 * Tape interface routines
 */

static unsigned int
getlen(Tuplestorestate *state, bool eofOK)
{
	unsigned int len;
	size_t		nbytes;

	nbytes = BufFileRead(state->myfile, (void *) &len, sizeof(len));
	if (nbytes == sizeof(len))
		return len;
	if (nbytes != 0 || !eofOK)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
						nbytes, sizeof(len))));
	return 0;
}


/*
 * Routines specialized for HeapTuple case
 *
 * The stored form is actually a MinimalTuple, but for largely historical
 * reasons we allow COPYTUP to work from a HeapTuple.
 *
 * Since MinimalTuple already has length in its first word, we don't need
 * to write that separately.
 */

static void *
copytup_heap(Tuplestorestate *state, void *tup)
{
	MinimalTuple tuple;

	tuple = minimal_tuple_from_heap_tuple((HeapTuple) tup);
	USEMEM(state, GetMemoryChunkSpace(tuple));
	return (void *) tuple;
}

static void
writetup_heap(Tuplestorestate *state, void *tup)
{
	MinimalTuple tuple = (MinimalTuple) tup;

	/* the part of the MinimalTuple we'll write: */
	char	   *tupbody = (char *) tuple + MINIMAL_TUPLE_DATA_OFFSET;
	unsigned int tupbodylen = tuple->t_len - MINIMAL_TUPLE_DATA_OFFSET;

	/* total on-disk footprint: */
	unsigned int tuplen = tupbodylen + sizeof(int);

	BufFileWrite(state->myfile, (void *) &tuplen, sizeof(tuplen));
	BufFileWrite(state->myfile, (void *) tupbody, tupbodylen);
	if (state->backward)		/* need trailing length word? */
		BufFileWrite(state->myfile, (void *) &tuplen, sizeof(tuplen));

	Size		memsize = GetMemoryChunkSpace(tuple);
	state->spilledBytes += memsize;
	FREEMEM(state, memsize);
	heap_free_minimal_tuple(tuple);
}

static void *
readtup_heap(Tuplestorestate *state, unsigned int len)
{
	unsigned int tupbodylen = len - sizeof(int);
	unsigned int tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET;
	MinimalTuple tuple = (MinimalTuple) palloc(tuplen);
	char	   *tupbody = (char *) tuple + MINIMAL_TUPLE_DATA_OFFSET;
	size_t		nread;

	USEMEM(state, GetMemoryChunkSpace(tuple));
	/* read in the tuple proper */
	tuple->t_len = tuplen;
	nread = BufFileRead(state->myfile, (void *) tupbody, tupbodylen);
	if (nread != (size_t) tupbodylen)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
						nread, (size_t) tupbodylen)));
	if (state->backward)		/* need trailing length word? */
	{
		nread = BufFileRead(state->myfile, (void *) &tuplen, sizeof(tuplen));
		if (nread != sizeof(tuplen))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
							nread, sizeof(tuplen))));
	}
	return (void *) tuple;
}

/*
 * tuplestore_set_instrument
 *
 * May be called after tuplestore_begin_xxx() to enable reporting of
 * statistics and events for EXPLAIN ANALYZE.
 *
 * The 'instr' ptr is retained in the 'state' object.  The caller must
 * ensure that it remains valid for the life of the Tuplestorestate object.
 */
void
tuplestore_set_instrument(Tuplestorestate *state,
						  struct Instrumentation *instrument)
{
	state->instrument = instrument;
}                               /* tuplestore_set_instrument */


/* Extra GPDB functions for sharing tuplestores across processes */

/*
 * tuplestore_make_shared
 *
 * Make a tuplestore available for sharing later. This must be called
 * immediately after tuplestore_begin_heap().
 */
void
tuplestore_make_shared(Tuplestorestate *state, SharedFileSet *fileset, const char *filename)
{
	ResourceOwner oldowner;

	state->work_set = workfile_mgr_create_set("SharedTupleStore", filename, true /* hold pin */);

	Assert(state->status == TSS_INMEM);
	Assert(state->tuples == 0);
	Assert(state->share_status == TSHARE_NOT_SHARED);
	state->share_status = TSHARE_WRITER;
	state->fileset = fileset;
	state->shared_filename = pstrdup(filename);

	/*
	 * Switch to tape-based operation, like in tuplestore_puttuple_common().
	 * We could delay this until tuplestore_freeze(), but we know we'll have
	 * to write everything to the file anyway, so let's not waste memory
	 * buffering the tuples in the meanwhile.
	 */
	PrepareTempTablespaces();

	/* associate the file with the store's resource owner */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = state->resowner;

	state->myfile = BufFileCreateShared(fileset, filename, state->work_set);
	CurrentResourceOwner = oldowner;

	/*
	 * For now, be conservative and always use trailing length words for
	 * cross-process tuplestores. It's important that the writer and the
	 * reader processes agree on this, and forcing it to true is the
	 * simplest way to achieve that.
	 */
	state->backward = true;
	state->status = TSS_WRITEFILE;
}

static void
writetup_forbidden(Tuplestorestate *state, void *tup)
{
	elog(ERROR, "cannot write to tuplestore, it is already frozen");
}

/*
 * tuplestore_freeze
 *
 * Flush the current buffer to disk, and forbid further inserts. This
 * prepares the tuplestore for reading from a different process.
 */
void
tuplestore_freeze(Tuplestorestate *state)
{
	Assert(state->share_status == TSHARE_WRITER);
	Assert(!state->frozen);
	dumptuples(state);
	BufFileExportShared(state->myfile);
	state->frozen = true;
}

/*
 * tuplestore_open_shared
 *
 * Open a shared tuplestore that has been populated in another process
 * for reading.
 */
Tuplestorestate *
tuplestore_open_shared(SharedFileSet *fileset, const char *filename)
{
	Tuplestorestate *state;
	int			eflags;

	eflags = EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND;

	state = tuplestore_begin_common(eflags,
									false /* interXact, ignored because we open existing files */,
									10 /* no need for memory buffers */);

	state->backward = true;

	state->copytup = copytup_heap;
	state->writetup = writetup_forbidden;
	state->readtup = readtup_heap;

	state->myfile = BufFileOpenShared(fileset, filename, O_RDONLY);
	state->readptrs[0].file = 0;
	state->readptrs[0].offset = 0L;
	state->status = TSS_READFILE;

	state->share_status = TSHARE_READER;
	state->frozen = false;
	state->fileset = fileset;
	state->shared_filename = pstrdup(filename);

	return state;
}
/*
 * When the remaining quantity  is greater than zero, it needs to return true.
 */
extern bool tuplestore_has_remaining_tuples(Tuplestorestate *state)
{
	return state->remaining_tuples > 0;
}

/*
 * Each time a tuple is processed, the counter of the number of remaining tuples is decremented.
 */
extern void tuplestore_consume_tuple(Tuplestorestate *state)
{
	--state->remaining_tuples;
}

/*
 * tuplestore_open_shared_noerror
 *
 * Open a shared tuplestore that has been populated in another process
 * for reading and ignore error when open an empty file.
 */
Tuplestorestate *
tuplestore_open_shared_noerror(SharedFileSet *fileset, const char *filename)
{
	Tuplestorestate *state;
	int			eflags;
	BufFile		*myfile;

	myfile = BufFileOpenSharedV2(fileset, filename, O_RDONLY);
	if (myfile == NULL)
		return NULL;

	eflags = EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND;

	state = tuplestore_begin_common(eflags,
									false /* interXact, ignored because we open existing files */,
									10 /* no need for memory buffers */);

	state->backward = true;

	state->copytup = copytup_heap;
	state->writetup = writetup_forbidden;
	state->readtup = readtup_heap;

	state->myfile = myfile;
	state->readptrs[0].file = 0;
	state->readptrs[0].offset = 0L;
	state->status = TSS_READFILE;

	state->share_status = TSHARE_READER;
	state->frozen = false;
	state->fileset = fileset;
	state->shared_filename = pstrdup(filename);

	return state;
}

/*
 * tuplestore_make_sharedV2
 *
 * Make a tuplestore for sharing. This can be called
 * after tuplestore_begin_heap().
 */
void
tuplestore_make_sharedV2(Tuplestorestate *state, SharedFileSet *fileset,
						const char *filename,
						ResourceOwner owner)
{
	ResourceOwner oldowner;
	MemoryContext oldctx;

	if (state->status == TSS_WRITEFILE)
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	state->work_set = workfile_mgr_create_set("SharedTupleStore", filename, true /* hold pin */);

	Assert(state->status == TSS_INMEM);
	Assert(state->share_status == TSHARE_NOT_SHARED);
	state->share_status = TSHARE_WRITER;
	state->fileset = fileset;
	if (state->shared_filename == NULL)
		state->shared_filename = pstrdup(filename);

	/*
	 * Switch to tape-based operation, like in tuplestore_puttuple_common().
	 * We could delay this until tuplestore_freeze(), but we know we'll have
	 * to write everything to the file anyway, so let's not waste memory
	 * buffering the tuples in the meanwhile.
	 */
	PrepareTempTablespaces();

	/* associate the file with the store's resource owner */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	state->myfile = BufFileCreateShared(fileset, filename, state->work_set);
	CurrentResourceOwner = oldowner;

	/*
	 * For now, be conservative and always use trailing length words for
	 * cross-process tuplestores. It's important that the writer and the
	 * reader processes agree on this, and forcing it to true is the
	 * simplest way to achieve that.
	 */
	state->backward = true;
	state->status = TSS_WRITEFILE;
	MemoryContextSwitchTo(oldctx);
}

/*
 * tuplestore_set_tuplecount
 *
 * Set the number of tuples in the tuplestore.
 */
void
tuplestore_set_tuplecount(Tuplestorestate *state, int64 tuplecount)
{
	state->tuples = tuplecount;
}

/*
 * tuplestore_get_sharedname
 *
 * Get the shared filename associated with the tuplestore.
 */
char *
tuplestore_get_sharedname(Tuplestorestate *state)
{
	return state->shared_filename;
}

/*
 * tuplestore_get_resowner
 *
 * Get the resource owner associated with the tuplestore.
 */
ResourceOwner
tuplestore_get_resowner(Tuplestorestate *state)
{
	return state->resowner;
}

/*
 * tuplestore_set_tableid
 *
 * Set the table OID associated with the tuplestore.
 */
void
tuplestore_set_tableid(Tuplestorestate *state, Oid tableid)
{
	state->tableid = tableid;
}

/*
 * tuplestore_set_sharedname
 *
 * Set the shared filename associated with the tuplestore.
 */
void
tuplestore_set_sharedname(Tuplestorestate *state, char *sharedname)
{
	state->shared_filename = pstrdup(sharedname);
}

/*
 * tuplestore_in_freezed
 *
 * Check if the tuplestore is frozen.
 */
bool
tuplestore_in_freezed(Tuplestorestate *state)
{
	return state->frozen == true;
}

/*
 * tuplestore_set_flags
 *
 * Set the flags for the tuplestore.
 */
void
tuplestore_set_flags(Tuplestorestate *state, bool isTemp)
{
	/* Set the file as a temporary file */
	BufFileSetIsTempFile(state->myfile, true);
}
