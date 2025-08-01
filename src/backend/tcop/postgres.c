/*-------------------------------------------------------------------------
 *
 * postgres.c
 *	  POSTGRES C Backend Interface
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/tcop/postgres.c
 *
 * NOTES
 *	  this is the "main" module of the postgres backend and
 *	  hence the main module of the "traffic cop".
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#include <pthread.h>

#include "access/parallel.h"
#include "access/printtup.h"
#include "access/xact.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "crypto/bufenc.h"
#include "crypto/kmgr.h"
#include "jit/jit.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "mb/stringinfo_mb.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"            /* Slice, SliceTable */
#include "nodes/print.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "pg_getopt.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tcop/fastpath.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/backend_cancel.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbgang.h"
#include "cdb/ml_ipc.h"
#include "access/twophase.h"
#include "postmaster/backoff.h"
#include "postmaster/fts.h"
#include "utils/guc.h"
#include "utils/resource_manager.h"
#include "utils/session_state.h"
#include "utils/vmem_tracker.h"
#include "tcop/idle_resource_cleaner.h"
#include "commands/matview.h"

/* ----------------
 *		global variables
 * ----------------
 */
const char *debug_query_string; /* client-supplied query string */

/* Note: whereToSendOutput is initialized for the bootstrap/standalone case */
CommandDest whereToSendOutput = DestDebug;

/* flag for logging end of session */
bool		Log_disconnections = false;

int			log_statement = LOGSTMT_NONE;

/* GUC variable for maximum stack depth (measured in kilobytes) */
int			max_stack_depth = 100;

/* wait N seconds to allow attach from a debugger */
int			PostAuthDelay = 0;

/* Time between checks that the client is still connected. */
int			client_connection_check_interval = 0;

/*
 * Hook for extensions, to get notified when query cancel or DIE signal is
 * received. This allows the extension to stop whatever it's doing as
 * quickly as possible. Normally, you would sprinkle your code with
 * CHECK_FOR_INTERRUPTS() in suitable places, but sometimes that's not
 * possible, for example because you call a slow function in a 3rd party
 * library that you have no control over. In the hook function, you might
 * be able to abort such a slow operation somehow.
 *
 * This gets called after setting ProcDiePending, QueryCancelPending, so
 * the hook function can check those to determine what event happened.
 */
cancel_pending_hook_type cancel_pending_hook = NULL;

/*
 * Hook for query execution.
 */
exec_simple_query_hook_type exec_simple_query_hook = NULL;

/* ----------------
 *		private typedefs etc
 * ----------------
 */

/* type of argument for bind_param_error_callback */
typedef struct BindParamCbData
{
	const char *portalName;
	int			paramno;		/* zero-based param number, or -1 initially */
	const char *paramval;		/* textual input string, if available */
} BindParamCbData;

/* ----------------
 *		private variables
 * ----------------
 */

/* Priority of the postmaster process */
static int PostmasterPriority = 0;

/* max_stack_depth converted to bytes for speed of checking */
static long max_stack_depth_bytes = 100 * 1024L;

/*
 * Stack base pointer -- initialized by PostmasterMain and inherited by
 * subprocesses. This is not static because old versions of PL/Java modify
 * it directly. Newer versions use set_stack_base(), but we want to stay
 * binary-compatible for the time being.
 */
char	   *stack_base_ptr = NULL;

/*
 * On IA64 we also have to remember the register stack base.
 */
#if defined(__ia64__) || defined(__ia64)
char	   *register_stack_base_ptr = NULL;
#endif

/*
 * Flag to keep track of whether we have started a transaction.
 * For extended query protocol this has to be remembered across messages.
 */
static bool xact_started = false;

/*
 * Flag to indicate that we are doing the outer loop's read-from-client,
 * as opposed to any random read from client that might happen within
 * commands like COPY FROM STDIN.
 *
 * GPDB:  I've made this extern so we can test it in the sigalarm handler
 * in proc.c.
 */
extern bool DoingCommandRead;
bool DoingCommandRead = false;

/*
 * Flags to implement skip-till-Sync-after-error behavior for messages of
 * the extended query protocol.
 */
static bool doing_extended_query_message = false;
static bool ignore_till_sync = false;

/*
 * If an unnamed prepared statement exists, it's stored here.
 * We keep it separate from the hashtable kept by commands/prepare.c
 * in order to reduce overhead for short-lived queries.
 */
static CachedPlanSource *unnamed_stmt_psrc = NULL;

/* assorted command-line switches */
static const char *userDoption = NULL;	/* -D switch */
static bool EchoQuery = false;	/* -E switch */
static bool UseSemiNewlineNewline = false;	/* -j switch */

#ifndef _WIN32
pthread_t main_tid = (pthread_t)0;
#else
pthread_t main_tid = {0,0};
#endif

/* if we're in the middle of dying, let our threads exit with some dignity */
static volatile sig_atomic_t in_quickdie = false;

/* whether or not, and why, we were canceled by conflict with recovery */
static bool RecoveryConflictPending = false;
static bool RecoveryConflictRetryable = true;
static ProcSignalReason RecoveryConflictReason;

/* reused buffer to pass to SendRowDescriptionMessage() */
static MemoryContext row_description_context = NULL;
static StringInfoData row_description_buf;

static DtxContextInfo TempDtxContextInfo = DtxContextInfo_StaticInit;

/* ----------------------------------------------------------------
 *		decls for routines only used in this file
 * ----------------------------------------------------------------
 */
static int	InteractiveBackend(StringInfo inBuf);
static int	interactive_getc(void);
static int	SocketBackend(StringInfo inBuf);
static int	ReadCommand(StringInfo inBuf);
static void forbidden_in_wal_sender(int firstchar);
static bool check_log_statement(List *stmt_list);
static int	errdetail_execute(List *raw_parsetree_list);
static int	errdetail_params(ParamListInfo params);
static int	errdetail_abort(void);
static int	errdetail_recovery_conflict(void);
static void bind_param_error_callback(void *arg);
static void start_xact_command(void);
static void finish_xact_command(void);
static bool IsTransactionExitStmt(Node *parsetree);
static bool IsTransactionExitStmtList(List *pstmts);
static bool IsTransactionStmtList(List *pstmts);
static void drop_unnamed_stmt(void);
static void log_disconnections(int code, Datum arg);
static void enable_statement_timeout(void);
static void disable_statement_timeout(void);
static bool CheckDebugDtmActionSqlCommandTag(const char *sqlCommandTag);
static bool CheckDebugDtmActionProtocol(DtxProtocolCommand dtxProtocolCommand,
					DtxContextInfo *contextInfo);
static bool renice_current_process(int nice_level);

/*
 * Change the priority of the current process to the specified level
 * (bigger nice_level values correspond to lower priority).
*/
static bool renice_current_process(int nice_level)
{
#ifdef WIN32
	elog(DEBUG2, "Renicing of processes on Windows currently not supported.");
	return false;
#else
	int prio_out = -1;
	elog(DEBUG2, "Current nice level of the process: %d",
			getpriority(PRIO_PROCESS, 0));
	prio_out = setpriority(PRIO_PROCESS, 0, nice_level);
	if (prio_out == -1)
	{
		int save_errno = errno;
		switch (save_errno)
		{
		case EACCES:
			elog(DEBUG1, "Could not change priority of the query process, errno: %d (%m).",
				 save_errno);
			break;
		case ESRCH:
			/* ignore this, the backend went away when we weren't looking */
			break;
		default:
			elog(DEBUG1, "Could not change priority of the query process, errno: %d (%m).",
				 save_errno);
		}
		return false;
	}

	elog(DEBUG2, "Reniced process to level %d", getpriority(PRIO_PROCESS, 0));
	return true;
#endif
}

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/* ----------------
 *	InteractiveBackend() is called for user interactive connections
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */

static int
InteractiveBackend(StringInfo inBuf)
{
	int			c;				/* character read from getc() */

	/*
	 * display a prompt and obtain input from the user
	 */
	printf("backend> ");
	fflush(stdout);

	resetStringInfo(inBuf);

	/*
	 * Read characters until EOF or the appropriate delimiter is seen.
	 */
	while ((c = interactive_getc()) != EOF)
	{
		if (c == '\n')
		{
			if (UseSemiNewlineNewline)
			{
				/*
				 * In -j mode, semicolon followed by two newlines ends the
				 * command; otherwise treat newline as regular character.
				 */
				if (inBuf->len > 1 &&
					inBuf->data[inBuf->len - 1] == '\n' &&
					inBuf->data[inBuf->len - 2] == ';')
				{
					/* might as well drop the second newline */
					break;
				}
			}
			else
			{
				/*
				 * In plain mode, newline ends the command unless preceded by
				 * backslash.
				 */
				if (inBuf->len > 0 &&
					inBuf->data[inBuf->len - 1] == '\\')
				{
					/* discard backslash from inBuf */
					inBuf->data[--inBuf->len] = '\0';
					/* discard newline too */
					continue;
				}
				else
				{
					/* keep the newline character, but end the command */
					appendStringInfoChar(inBuf, '\n');
					break;
				}
			}
		}

		/* Not newline, or newline treated as regular character */
		appendStringInfoChar(inBuf, (char) c);
	}

	/* No input before EOF signal means time to quit. */
	if (c == EOF && inBuf->len == 0)
		return EOF;

	/*
	 * otherwise we have a user query so process it.
	 */

	/* Add '\0' to make it look the same as message case. */
	appendStringInfoChar(inBuf, (char) '\0');

	/*
	 * if the query echo flag was given, print the query..
	 */
	if (EchoQuery)
		printf("statement: %s\n", inBuf->data);
	fflush(stdout);

	return 'Q';
}

/*
 * interactive_getc -- collect one character from stdin
 *
 * Even though we are not reading from a "client" process, we still want to
 * respond to signals, particularly SIGTERM/SIGQUIT.
 */
static int
interactive_getc(void)
{
	int			c;

	/*
	 * This will not process catchup interrupts or notifications while
	 * reading. But those can't really be relevant for a standalone backend
	 * anyway. To properly handle SIGTERM there's a hack in die() that
	 * directly processes interrupts at this stage...
	 */
	CHECK_FOR_INTERRUPTS();

	enable_client_wait_timeout_interrupt();

	c = getc(stdin);

	disable_client_wait_timeout_interrupt();

	ProcessClientReadInterrupt(false);

	return c;
}

/* ----------------
 *	SocketBackend()		Is called for frontend-backend connections
 *
 *	Returns the message type code, and loads message body data into inBuf.
 *
 *	EOF is returned if the connection is lost.
 * ----------------
 */
static int
SocketBackend(StringInfo inBuf)
{
	int			qtype;
	int			maxmsglen;

	/*
	 * Get message type code from the frontend.
	 */
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	qtype = pq_getbyte();

	if (qtype == EOF)			/* frontend disconnected */
	{
		if (IsTransactionState())
			ereport(COMMERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unexpected EOF on client connection with an open transaction")));
		else
		{
			/*
			 * Can't send DEBUG log messages to client at this point. Since
			 * we're disconnecting right away, we don't need to restore
			 * whereToSendOutput.
			 */
			whereToSendOutput = DestNone;
			ereport(DEBUG1,
					(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
					 errmsg_internal("unexpected EOF on client connection")));
		}
		return qtype;
	}

	/*
	 * Validate message type code before trying to read body; if we have lost
	 * sync, better to say "command unknown" than to run out of memory because
	 * we used garbage as a length word.  We can also select a type-dependent
	 * limit on what a sane length word could be.  (The limit could be chosen
	 * more granularly, but it's not clear it's worth fussing over.)
	 *
	 * This also gives us a place to set the doing_extended_query_message flag
	 * as soon as possible.
	 */
	switch (qtype)
	{
		case 'Q':				/* simple query */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = false;
			break;

		case 'M':				/* Apache Cloudberry dispatched statement from QD */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = false;

			/* don't support old protocols with this. */
			if( PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 )
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("dispatch unsupported for old FrontendProtocols")));


			break;

		case 'T':				/* Apache Cloudberry dispatched transaction protocol from QD */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = false;

			/* don't support old protocols with this. */
			if( PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 )
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("dispatch unsupported for old FrontendProtocols")));


			break;

		case 'F':				/* fastpath function call */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = false;
			break;

		case 'X':				/* terminate */
			maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
			doing_extended_query_message = false;
			ignore_till_sync = false;
			break;

		case 'B':				/* bind */
		case 'P':				/* parse */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = true;
			break;

		case 'C':				/* close */
		case 'D':				/* describe */
		case 'E':				/* execute */
		case 'H':				/* flush */
			maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
			doing_extended_query_message = true;
			break;

		case 'S':				/* sync */
			maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
			/* stop any active skip-till-Sync */
			ignore_till_sync = false;
			/* mark not-extended, so that a new error doesn't begin skip */
			doing_extended_query_message = false;
			break;

		case 'd':				/* copy data */
			maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
			doing_extended_query_message = false;
			break;

		case 'c':				/* copy done */
		case 'f':				/* copy fail */
		case '?':				/* Cloudberry sequence response */
			maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
			doing_extended_query_message = false;
			break;

		default:

			/*
			 * Otherwise we got garbage from the frontend.  We treat this as
			 * fatal because we have probably lost message boundary sync, and
			 * there's no good way to recover.
			 */
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid frontend message type %d", qtype)));
			maxmsglen = 0;		/* keep compiler quiet */
			break;
	}

	/*
	 * In protocol version 3, all frontend messages have a length word next
	 * after the type code; we can read the message contents independently of
	 * the type.
	 */
	if (pq_getmessage(inBuf, maxmsglen))
		return EOF;				/* suitable message already logged */
	RESUME_CANCEL_INTERRUPTS();

	return qtype;
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(StringInfo inBuf)
{
	int			result;

	/*
	 * XXX Use of this fault is discouraged!  This fault location is reached
	 * in query executing backends as well as many non-query executing
	 * processes such as FTS probe handler, walsender, etc.  It is also found
	 * to be triggered by the same SQL statement used to inject the fault,
	 * causing difficult to analyse failures in CI.  If a test intends to
	 * target a query executing backend process, consider using
	 * "exec_simple_query_start" fault.
	 */
	SIMPLE_FAULT_INJECTOR("before_read_command");

	if (whereToSendOutput == DestRemote)
		result = SocketBackend(inBuf);
	else
		result = InteractiveBackend(inBuf);
	return result;
}

/*
 * ProcessClientReadInterrupt() - Process interrupts specific to client reads
 *
 * This is called just before and after low-level reads.
 * 'blocked' is true if no data was available to read and we plan to retry,
 * false if about to read or done reading.
 *
 * Must preserve errno!
 */
void
ProcessClientReadInterrupt(bool blocked)
{
	int			save_errno = errno;

	if (DoingCommandRead)
	{
		/* Check for general interrupts that arrived before/while reading */
		CHECK_FOR_INTERRUPTS();

		/* Process sinval catchup interrupts, if any */
		if (catchupInterruptPending)
			ProcessCatchupInterrupt();

		/* Process notify interrupts, if any */
		if (notifyInterruptPending)
			ProcessNotifyInterrupt(true);
	}
	else if (ProcDiePending)
	{
		/*
		 * We're dying.  If there is no data available to read, then it's safe
		 * (and sane) to handle that now.  If we haven't tried to read yet,
		 * make sure the process latch is set, so that if there is no data
		 * then we'll come back here and die.  If we're done reading, also
		 * make sure the process latch is set, as we might've undesirably
		 * cleared it while reading.
		 */
		if (blocked)
			CHECK_FOR_INTERRUPTS();
		else
			SetLatch(MyLatch);
	}

	errno = save_errno;
}

/*
 * ProcessClientWriteInterrupt() - Process interrupts specific to client writes
 *
 * This is called just before and after low-level writes.
 * 'blocked' is true if no data could be written and we plan to retry,
 * false if about to write or done writing.
 *
 * Must preserve errno!
 */
void
ProcessClientWriteInterrupt(bool blocked)
{
	int			save_errno = errno;

	if (ProcDiePending)
	{
		/*
		 * We're dying.  If it's not possible to write, then we should handle
		 * that immediately, else a stuck client could indefinitely delay our
		 * response to the signal.  If we haven't tried to write yet, make
		 * sure the process latch is set, so that if the write would block
		 * then we'll come back here and die.  If we're done writing, also
		 * make sure the process latch is set, as we might've undesirably
		 * cleared it while writing.
		 */
		if (blocked)
		{
			/*
			 * Don't mess with whereToSendOutput if ProcessInterrupts wouldn't
			 * service ProcDiePending.
			 */
			if (InterruptHoldoffCount == 0 && CritSectionCount == 0)
			{
				/*
				 * We don't want to send the client the error message, as a)
				 * that would possibly block again, and b) it would likely
				 * lead to loss of protocol sync because we may have already
				 * sent a partial protocol message.
				 */
				if (whereToSendOutput == DestRemote)
					whereToSendOutput = DestNone;

				CHECK_FOR_INTERRUPTS();
			}
		}
		else
			SetLatch(MyLatch);
	}

	errno = save_errno;
}

/*
 * Do raw parsing (only).
 *
 * A list of parsetrees (RawStmt nodes) is returned, since there might be
 * multiple commands in the given string.
 *
 * NOTE: for interactive queries, it is important to keep this routine
 * separate from the analysis & rewrite stages.  Analysis and rewriting
 * cannot be done in an aborted transaction, since they require access to
 * database tables.  So, we rely on the raw parser to determine whether
 * we've seen a COMMIT or ABORT command; when we are in abort state, other
 * commands are not processed any further than the raw parse stage.
 */
List *
pg_parse_query(const char *query_string)
{
	List	   *raw_parsetree_list;

	TRACE_POSTGRESQL_QUERY_PARSE_START(query_string);

	if (log_parser_stats)
		ResetUsage();

	raw_parsetree_list = raw_parser(query_string, RAW_PARSE_DEFAULT);

	if (log_parser_stats)
		ShowUsage("PARSER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass raw parsetrees through copyObject() */
	{
		List	   *new_list = copyObject(raw_parsetree_list);

		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, raw_parsetree_list))
			elog(WARNING, "copyObject() failed to produce an equal raw parse tree");
		else
			raw_parsetree_list = new_list;
	}
#endif

	/*
	 * Currently, outfuncs/readfuncs support is missing for many raw parse
	 * tree nodes, so we don't try to implement WRITE_READ_PARSE_PLAN_TREES
	 * here.
	 */

	TRACE_POSTGRESQL_QUERY_PARSE_DONE(query_string);

	return raw_parsetree_list;
}

/*
 * Given a raw parsetree (gram.y output), and optionally information about
 * types of parameter symbols ($n), perform parse analysis and rule rewriting.
 *
 * A list of Query nodes is returned, since either the analyzer or the
 * rewriter might expand one query to several.
 *
 * NOTE: for reasons mentioned above, this must be separate from raw parsing.
 */
List *
pg_analyze_and_rewrite(RawStmt *parsetree, const char *query_string,
					   Oid *paramTypes, int numParams,
					   QueryEnvironment *queryEnv)
{
	Query	   *query;
	List	   *querytree_list;

	TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	query = parse_analyze(parsetree, query_string, paramTypes, numParams,
						  queryEnv);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

	/*
	 * (2) Rewrite the queries, as necessary
	 */
	querytree_list = pg_rewrite_query(query);

	TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

	return querytree_list;
}

/*
 * Do parse analysis and rewriting.  This is the same as pg_analyze_and_rewrite
 * except that external-parameter resolution is determined by parser callback
 * hooks instead of a fixed list of parameter datatypes.
 */
List *
pg_analyze_and_rewrite_params(RawStmt *parsetree,
							  const char *query_string,
							  ParserSetupHook parserSetup,
							  void *parserSetupArg,
							  QueryEnvironment *queryEnv)
{
	ParseState *pstate;
	Query	   *query;
	List	   *querytree_list;
	JumbleState *jstate = NULL;

	Assert(query_string != NULL);	/* required as of 8.4 */

	TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_string;
	pstate->p_queryEnv = queryEnv;
	(*parserSetup) (pstate, parserSetupArg);

	query = transformTopLevelStmt(pstate, parsetree);

	if (IsQueryIdEnabled())
		jstate = JumbleQuery(query, query_string);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);

	pgstat_report_query_id(query->queryId, false);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

	/*
	 * (2) Rewrite the queries, as necessary
	 */
	querytree_list = pg_rewrite_query(query);

	TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

	return querytree_list;
}

/*
 * Perform rewriting of a query produced by parse analysis.
 *
 * Note: query must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on it.
 */
List *
pg_rewrite_query(Query *query)
{
	List	   *querytree_list;

	if (Debug_print_parse)
		elog_node_display(LOG, "parse tree", query,
						  Debug_pretty_print);

	if (log_parser_stats)
		ResetUsage();

	if (query->commandType == CMD_UTILITY)
	{
		/* don't rewrite utilities, just dump 'em into result list */
		querytree_list = list_make1(query);
	}
	else
	{
		/* rewrite regular queries */
		querytree_list = QueryRewrite(query);
	}

	if (log_parser_stats)
		ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass querytree through copyObject() */
	{
		List	   *new_list;

		new_list = copyObject(querytree_list);
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, querytree_list))
			elog(WARNING, "copyObject() failed to produce equal parse tree");
		else
			querytree_list = new_list;
	}
#endif

#ifdef WRITE_READ_PARSE_PLAN_TREES
	/* Optional debugging check: pass querytree through outfuncs/readfuncs */
	{
		List	   *new_list = NIL;
		ListCell   *lc;

		/*
		 * We currently lack outfuncs/readfuncs support for most utility
		 * statement types, so only attempt to write/read non-utility queries.
		 */
		foreach(lc, querytree_list)
		{
			Query	   *query = castNode(Query, lfirst(lc));

			if (query->commandType != CMD_UTILITY)
			{
				char	   *str = nodeToString(query);
				Query	   *new_query = stringToNodeWithLocations(str);

				/*
				 * queryId is not saved in stored rules, but we must preserve
				 * it here to avoid breaking pg_stat_statements.
				 */
				new_query->queryId = query->queryId;

				new_list = lappend(new_list, new_query);
				pfree(str);
			}
			else
				new_list = lappend(new_list, query);
		}

		/* This checks both outfuncs/readfuncs and the equal() routines... */
		if (!equal(new_list, querytree_list))
			elog(WARNING, "outfuncs/readfuncs failed to produce equal parse tree");
		else
			querytree_list = new_list;
	}
#endif

	if (Debug_print_rewritten)
		elog_node_display(LOG, "rewritten parse tree", querytree_list,
						  Debug_pretty_print);

	return querytree_list;
}


/*
 * Generate a plan for a single already-rewritten query.
 * This is a thin wrapper around planner() and takes the same parameters.
 */
PlannedStmt *
pg_plan_query(Query *querytree, const char *query_string, int cursorOptions,
			  ParamListInfo boundParams)
{
	PlannedStmt *plan;

	/* Utility commands have no plans. */
	if (querytree->commandType == CMD_UTILITY)
		return NULL;

	/* Planner must have a snapshot in case it calls user-defined functions. */
	Assert(ActiveSnapshotSet());

	TRACE_POSTGRESQL_QUERY_PLAN_START();

	if (log_planner_stats)
		ResetUsage();

	/* call the optimizer */
	plan = planner(querytree, query_string, cursorOptions, boundParams);

	if (log_planner_stats)
		ShowUsage("PLANNER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass plan tree through copyObject() */
	{
		PlannedStmt *new_plan = copyObject(plan);

		/*
		 * equal() currently does not have routines to compare Plan nodes, so
		 * don't try to test equality here.  Perhaps fix someday?
		 */
#ifdef NOT_USED
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_plan, plan))
			elog(WARNING, "copyObject() failed to produce an equal plan tree");
		else
#endif
			plan = new_plan;
	}
#endif

#ifdef WRITE_READ_PARSE_PLAN_TREES
	/* Optional debugging check: pass plan tree through outfuncs/readfuncs */
	{
		char	   *str;
		PlannedStmt *new_plan;

		str = nodeToString(plan);
		new_plan = stringToNodeWithLocations(str);
		pfree(str);

		/*
		 * equal() currently does not have routines to compare Plan nodes, so
		 * don't try to test equality here.  Perhaps fix someday?
		 */
#ifdef NOT_USED
		/* This checks both outfuncs/readfuncs and the equal() routines... */
		if (!equal(new_plan, plan))
			elog(WARNING, "outfuncs/readfuncs failed to produce an equal plan tree");
		else
#endif
			plan = new_plan;
	}
#endif

	/*
	 * Print plan if debugging.
	 */
	if (Debug_print_plan)
		elog_node_display(LOG, "plan", plan, Debug_pretty_print);

	TRACE_POSTGRESQL_QUERY_PLAN_DONE();

	return plan;
}

/*
 * Generate plans for a list of already-rewritten queries.
 *
 * For normal optimizable statements, invoke the planner.  For utility
 * statements, just make a wrapper PlannedStmt node.
 *
 * The result is a list of PlannedStmt nodes.
 */
List *
pg_plan_queries(List *querytrees, const char *query_string, int cursorOptions,
				ParamListInfo boundParams)
{
	List	   *stmt_list = NIL;
	ListCell   *query_list;

	foreach(query_list, querytrees)
	{
		Query	   *query = lfirst_node(Query, query_list);
		PlannedStmt *stmt;

		if (query->commandType == CMD_UTILITY)
		{
			/* Utility commands require no planning. */
			stmt = makeNode(PlannedStmt);
			stmt->commandType = CMD_UTILITY;
			stmt->canSetTag = query->canSetTag;
			stmt->utilityStmt = query->utilityStmt;
			stmt->stmt_location = query->stmt_location;
			stmt->stmt_len = query->stmt_len;
			stmt->queryId = query->queryId;
		}
		else
		{
			stmt = pg_plan_query(query, query_string, cursorOptions,
								 boundParams);
		}

		stmt_list = lappend(stmt_list, stmt);
	}

	return stmt_list;
}

/*
 * exec_mpp_query
 *
 * Called in a qExec process to read and execute a query plan sent by CdbDispatchPlan().
 *
 * query_string -- optional query text (C string).
 * serializedPlantree[len] -- PlannedStmt node, or (NULL,0) if query provided.
 * serializedQueryDispatchDesc[len] -- QueryDispatchDesc node, or (NULL,0) if query provided.
 *
 * Caller may supply either a Query (representing utility command) or
 * a PlannedStmt (representing a planned DML command), but not both.
 */
static void
exec_mpp_query(const char *query_string,
			   const char * serializedPlantree, int serializedPlantreelen,
			   const char * serializedQueryDispatchDesc, int serializedQueryDispatchDesclen)
{
	CommandDest dest = whereToSendOutput;
	MemoryContext oldcontext;
	bool		save_log_statement_stats = log_statement_stats;
	bool		was_logged = false;
	char		msec_str[32];
	PlannedStmt	   *plan = NULL;
	QueryDispatchDesc *ddesc = NULL;
	CmdType		commandType = CMD_UNKNOWN;
	SliceTable *sliceTable = NULL;
	ExecSlice  *slice = NULL;
	ParamListInfo paramLI = NULL;

	SIMPLE_FAULT_INJECTOR("exec_mpp_query_start");

	Assert(Gp_role == GP_ROLE_EXECUTE);
	/*
	 * If we didn't get passed a query string, dummy something up for ps display and pg_stat_activity
	 */
	if (query_string == NULL || strlen(query_string)==0)
		query_string = "mppexec";

	/*
	 * Report query to various monitoring facilities.
	 */

	debug_query_string = query_string;

	pgstat_report_activity(STATE_RUNNING, query_string);

	/*
	 * We use save_log_statement_stats so ShowUsage doesn't report incorrect
	 * results because ResetUsage wasn't called.
	 */
	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command.	All queries generated by the
	 * query_string will be in this same command block, *unless* we find a
	 * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
	 * one of those, else bad things will happen in xact.c. (Note that this
	 * will normally change current memory context.)
	 */
	start_xact_command();

	/*
	 * Zap any pre-existing unnamed statement.	(While not strictly necessary,
	 * it seems best to define simple-Query mode as if it used the unnamed
	 * statement and portal; this ensures we recover any storage used by prior
	 * unnamed operations.)
	 */
	drop_unnamed_stmt();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

 	/*
     * Deserialize the query execution plan (a PlannedStmt node), if there is one.
     */
	if (serializedPlantree != NULL && serializedPlantreelen > 0)
	{
		plan = (PlannedStmt *) deserializeNode(serializedPlantree,serializedPlantreelen);
		if (!plan || !IsA(plan, PlannedStmt))
			elog(ERROR, "MPPEXEC: receive invalid planned statement");
    }

	/*
     * Deserialize the extra execution information (a QueryDispatchDesc node), if there is one.
     */
    if (serializedQueryDispatchDesc != NULL && serializedQueryDispatchDesclen > 0)
    {
		ddesc = (QueryDispatchDesc *) deserializeNode(serializedQueryDispatchDesc,serializedQueryDispatchDesclen);
		if (!ddesc || !IsA(ddesc, QueryDispatchDesc))
			elog(ERROR, "MPPEXEC: received invalid QueryDispatchDesc with planned statement");
		/*
		 * Deserialize and apply security context from QD.
		 */
		SetUserIdAndSecContext(GetUserId(), ddesc->secContext);

        sliceTable = ddesc->sliceTable;

		if (sliceTable)
		{
			int			i;

			if (!IsA(sliceTable, SliceTable) ||
				sliceTable->localSlice < 0 ||
				sliceTable->localSlice >= sliceTable->numSlices)
				elog(ERROR, "MPPEXEC: received invalid slice table: %d", sliceTable->localSlice);

			/* Identify slice to execute */
			for (i = 0; i < sliceTable->numSlices; i++)
			{
				slice = &sliceTable->slices[i];

				if (bms_is_member(qe_identifier, slice->processesMap))
					break;
			}
			if (i == sliceTable->numSlices)
				elog(ERROR, "could not find QE identifier in process map");
			sliceTable->localSlice = slice->sliceIndex;

			/* Set global sliceid variable for elog. */
			currentSliceId = sliceTable->localSlice;
		}

		if (ddesc->oidAssignments)
			AddPreassignedOids(ddesc->oidAssignments);
    }

	if ( !plan )
		elog(ERROR, "MPPEXEC: received neither Query nor Plan");

	/* Extract command type from the planned statement. */
	if (plan->commandType != CMD_SELECT &&
		plan->commandType != CMD_INSERT &&
		plan->commandType != CMD_UPDATE &&
		plan->commandType != CMD_DELETE &&
		plan->commandType != CMD_UTILITY)
		elog(ERROR, "MPPEXEC: received non-DML Plan");
	commandType = plan->commandType;

	if ( slice )
	{
		/* Non root slices don't need update privileges. */
		if (sliceTable->localSlice != slice->rootIndex)
		{
			ListCell       *rtcell;
			RangeTblEntry  *rte;
			AclMode         removeperms = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_SELECT_FOR_UPDATE;

			/* Just reading, so don't check INS/DEL/UPD permissions. */
			foreach(rtcell, plan->rtable)
			{
				rte = (RangeTblEntry *)lfirst(rtcell);
				if (rte->rtekind == RTE_RELATION &&
					0 != (rte->requiredPerms & removeperms))
					rte->requiredPerms &= ~removeperms;
			}
		}
	}

	if (log_statement != LOGSTMT_NONE)
	{
	    /*
	    * TODO need to log SELECT INTO as DDL
	    */
	    if (log_statement == LOGSTMT_ALL ||
	            (plan->utilityStmt && log_statement == LOGSTMT_DDL) ||
	            (plan && log_statement >= LOGSTMT_MOD))

        {
            ereport(LOG, (errmsg("statement: %s", query_string)
                                          ));
            was_logged = true;
        }
	}

	/*
	 * Get (possibly 0) PARAM_EXTERN parameters. (PARAM_EXEC parameter
	 * will be handled later, in InitPlan()).
	 */
	if (ddesc && ddesc->paramInfo)
		paramLI = deserializeExternParams(ddesc->paramInfo);
	else
		paramLI = NULL;

	if (ddesc && ddesc->snaplen > 0)
	{
		AddPreassignedMVEntry(ddesc->matviewOid, ddesc->tableid, ddesc->snapname);
	}
	/*
	 * Switch back to transaction context to enter the loop.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * All unpacked and checked.  Process the command.
	 */
	{
		const char *commandName;
		QueryCompletion qc;

		Portal		portal;
		DestReceiver *receiver;
		int16		format;

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).	Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		if (commandType == CMD_UTILITY)
			commandName = "MPPEXEC UTILITY";
		else if (commandType == CMD_SELECT)
			commandName = "MPPEXEC SELECT";
		else if (commandType == CMD_INSERT)
			commandName = "MPPEXEC INSERT";
		else if (commandType == CMD_UPDATE)
			commandName = "MPPEXEC UPDATE";
		else if (commandType == CMD_DELETE)
			commandName = "MPPEXEC DELETE";
		else
			elog(ERROR, "MPPEXEC: received non-DML Plan");

		qc.commandTag = CreateCommandTag((Node *) plan);

		set_ps_display(commandName);

		BeginCommand(qc.commandTag, dest);

        /* Downgrade segworker process priority */
		if (gp_segworker_relative_priority != 0)
		{
			renice_current_process(PostmasterPriority + gp_segworker_relative_priority);
		}

		if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND &&
			CheckDebugDtmActionSqlCommandTag(commandName))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FAULT_INJECT),
					 errmsg("Raise ERROR for debug_dtm_action = %d, commandTag = %s",
							Debug_dtm_action, commandName)));
		}

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() /*&&*/
			/*!IsTransactionExitStmt(parsetree)*/)
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block")));

		/* Make sure we are in a transaction command */
		start_xact_command();

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing querytrees (again,
		 * these must outlive the execution context).
		 */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		CHECK_FOR_INTERRUPTS();

		/*
		 * Create unnamed portal to run the query or queries in. If there
		 * already is one, silently drop it.
		 */
		portal = CreatePortal("", true, true);
		/* Don't display the portal in pg_cursors */
		portal->visible = false;

		/*
		 * We don't have to copy anything into the portal, because everything
		 * we are passing here is in MessageContext, which will outlive the
		 * portal anyway.
		 */
		PortalDefineQuery(portal,
						  NULL,
						  query_string,
						  /*
						   * sourceTag is stored in parsetree, but the original parsetree isn't
						   * dispatched to QE, so set a generic T_Query here.
						   */
						  T_Query,
						  qc.commandTag,
						  list_make1(plan),
						  NULL);

		/*
		 * Start the portal.
		 */
		PortalStart(portal, paramLI, 0, InvalidSnapshot, ddesc);

		/*
		 * Select text output format, the default.
		 */
		format = 0;
		PortalSetResultFormat(portal, 1, &format);

		/*
		 * Now we can create the destination receiver object.
		 */
		receiver = CreateDestReceiver(dest);
		if (dest == DestRemote)
			SetRemoteDestReceiverParams(receiver, portal);

		/*
		 * Switch back to transaction context for execution.
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Run the portal to completion, and then drop it (and the receiver).
		 */
		(void) PortalRun(portal,
						 FETCH_ALL,
						 true, /* Effectively always top level. */
						 true,
						 receiver,
						 receiver,
						 &qc);

		/*
		 * If writer QE, sent current pgstat for tables to QD.
		 */
		if (Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
			pgstat_send_qd_tabstats();

		(*receiver->rDestroy) (receiver);

		PortalDrop(portal, false);

		/*
		 * Close down transaction statement before reporting command-complete.
		 * This is so that any end-of-transaction errors are reported before
		 * the command-complete message is issued, to avoid confusing
		 * clients who will expect either a command-complete message or an
		 * error, not one and then the other.
		 */
		finish_xact_command();

		if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_END_COMMAND &&
			CheckDebugDtmActionSqlCommandTag(commandName))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FAULT_INJECT),
					 errmsg("Raise ERROR for debug_dtm_action = %d, commandTag = %s",
							Debug_dtm_action, commandName)));
		}

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		EndCommand(&qc, dest, false);
	}							/* end loop over parsetrees */

	/*
	 * Close down transaction statement, if one is open.
	 */
	finish_xact_command();

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(false)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  statement: %s",
							msec_str, query_string),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("QUERY STATISTICS");


	if (gp_enable_resqueue_priority)
	{
		BackoffBackendEntryExit();
	}

	debug_query_string = NULL;
}

static bool
CheckDebugDtmActionProtocol(DtxProtocolCommand dtxProtocolCommand,
				DtxContextInfo *contextInfo)
{
	if (Debug_dtm_action_nestinglevel == 0)
	{
		return (Debug_dtm_action_target == DEBUG_DTM_ACTION_TARGET_PROTOCOL &&
			Debug_dtm_action_protocol == dtxProtocolCommand &&
			Debug_dtm_action_segment == GpIdentity.segindex);
	}
	else
	{
		return (Debug_dtm_action_target == DEBUG_DTM_ACTION_TARGET_PROTOCOL &&
			Debug_dtm_action_protocol == dtxProtocolCommand &&
			Debug_dtm_action_segment == GpIdentity.segindex &&
			Debug_dtm_action_nestinglevel == contextInfo->nestingLevel);
	}
}

static void
exec_mpp_dtx_protocol_command(DtxProtocolCommand dtxProtocolCommand,
							  const char *loggingStr,
							  const char *gid,
							  DtxContextInfo *contextInfo)
{
	QueryCompletion	qc;
	CommandDest dest = whereToSendOutput;
	qc.commandTag = GetCommandTagEnum(loggingStr);
	qc.nprocessed = 1;

	SIMPLE_FAULT_INJECTOR("exec_dtx_protocol_start");

	if (log_statement == LOGSTMT_ALL)
		elog(LOG,"DTM protocol command '%s' for gid = %s", loggingStr, gid);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"exec_mpp_dtx_protocol_command received the dtxProtocolCommand = %d (%s) gid = %s",
		 dtxProtocolCommand, loggingStr, gid);

	set_ps_display(loggingStr);

	if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND &&
		CheckDebugDtmActionProtocol(dtxProtocolCommand, contextInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FAULT_INJECT),
				 errmsg("Raise ERROR for debug_dtm_action = %d, debug_dtm_action_protocol = %s",
						Debug_dtm_action, DtxProtocolCommandToString(dtxProtocolCommand))));
	}
	if (Debug_dtm_action == DEBUG_DTM_ACTION_PANIC_BEGIN_COMMAND &&
		CheckDebugDtmActionProtocol(dtxProtocolCommand, contextInfo))
	{
		/*
		 * Avoid core file generation for this PANIC. It helps to avoid
		 * filling up disks during tests and also saves time.
		 */
		AvoidCorefileGeneration();
		elog(PANIC,"PANIC for debug_dtm_action = %d, debug_dtm_action_protocol = %s",
			 Debug_dtm_action, DtxProtocolCommandToString(dtxProtocolCommand));
	}

	BeginCommand(qc.commandTag, dest);

	performDtxProtocolCommand(dtxProtocolCommand, gid, contextInfo);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"exec_mpp_dtx_protocol_command calling EndCommand for dtxProtocolCommand = %d (%s) gid = %s",
		 dtxProtocolCommand, loggingStr, gid);

	if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_END_COMMAND &&
		CheckDebugDtmActionProtocol(dtxProtocolCommand, contextInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FAULT_INJECT),
				 errmsg("Raise error for debug_dtm_action = %d, debug_dtm_action_protocol = %s",
						Debug_dtm_action, DtxProtocolCommandToString(dtxProtocolCommand))));
	}

	/*
	 * GPDB: There is a corner case that we need to delay connection
	 * termination to here. see SyncRepWaitForLSN() for details.
	 * */
	if (ProcDiePending)
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				errmsg("Terminating the connection (DTM protocol command '%s' "
					   "for gid=%s", loggingStr, gid)));

	EndCommand(&qc, dest, false);
}

static bool
CheckDebugDtmActionSqlCommandTag(const char *sqlCommandTag)
{
	bool result;

	result = (Debug_dtm_action_target == DEBUG_DTM_ACTION_TARGET_SQL &&
			  strcmp(Debug_dtm_action_sql_command_tag, sqlCommandTag) == 0 &&
			  Debug_dtm_action_segment == GpIdentity.segindex);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"CheckDebugDtmActionSqlCommandTag Debug_dtm_action_target = %d, Debug_dtm_action_sql_command_tag = '%s' check '%s', Debug_dtm_action_segment = %d, Debug_dtm_action_primary = %s, result = %s.",
		Debug_dtm_action_target,
		Debug_dtm_action_sql_command_tag, (sqlCommandTag == NULL ? "<NULL>" : sqlCommandTag),
		Debug_dtm_action_segment, (Debug_dtm_action_primary ? "true" : "false"),
		(result ? "true" : "false"));

	return result;
}

static void
restore_guc_to_QE(void )
{
	Assert(Gp_role == GP_ROLE_DISPATCH && gp_guc_restore_list);
	ListCell *lc;

	start_xact_command();

	foreach(lc, gp_guc_restore_list)
	{
		struct config_generic* gconfig = (struct config_generic *)lfirst(lc);
		PG_TRY();
		{
			DispatchSyncPGVariable(gconfig);
		}
		PG_CATCH();
		{
			/* if some guc can not restore successful
			 * we can not keep alive gang anymore.
			 */
			DisconnectAndDestroyAllGangs(false);
			/*
			 * when qe elog an error, qd will use ReThrowError to
			 * re throw the error, the errordata_stack_depth will ++,
			 * when we catch the error we should reset errordata_stack_depth
			 * by FlushErrorState.
			 */
			FlushErrorState();
		}
		PG_END_TRY();
	}

	finish_xact_command();
	list_free(gp_guc_restore_list);
	gp_guc_restore_list = NIL;
}

/*
 * exec_simple_query
 *
 * Execute a "simple Query" protocol message.
 */
void
exec_simple_query(const char *query_string)
{
	CommandDest dest = whereToSendOutput;
	MemoryContext oldcontext;
	List	   *parsetree_list;
	ListCell   *parsetree_item;
	bool		save_log_statement_stats = log_statement_stats;
	bool		was_logged = false;
	bool		use_implicit_block;
	char		msec_str[32];

	SIMPLE_FAULT_INJECTOR("exec_simple_query_start");

	if (Gp_role != GP_ROLE_EXECUTE)
		increment_command_count();

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;

	pgstat_report_activity(STATE_RUNNING, query_string);

	TRACE_POSTGRESQL_QUERY_START(query_string);

	/*
	 * We use save_log_statement_stats so ShowUsage doesn't report incorrect
	 * results because ResetUsage wasn't called.
	 */
	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command.  All queries generated by the
	 * query_string will be in this same command block, *unless* we find a
	 * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
	 * one of those, else bad things will happen in xact.c. (Note that this
	 * will normally change current memory context.)
	 */
	start_xact_command();

	/*
	 * Zap any pre-existing unnamed statement.  (While not strictly necessary,
	 * it seems best to define simple-Query mode as if it used the unnamed
	 * statement and portal; this ensures we recover any storage used by prior
	 * unnamed operations.)
	 */
	drop_unnamed_stmt();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);

	/* Log immediately if dictated by log_statement */
	if (check_log_statement(parsetree_list))
	{
		ereport(LOG,
				(errmsg("statement: %s", query_string),
				 errhidestmt(true),
				 errdetail_execute(parsetree_list)));
		was_logged = true;
	}

	/*
	 * Switch back to transaction context to enter the loop.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * For historical reasons, if multiple SQL statements are given in a
	 * single "simple Query" message, we execute them as a single transaction,
	 * unless explicit transaction control commands are included to make
	 * portions of the list be separate transactions.  To represent this
	 * behavior properly in the transaction machinery, we use an "implicit"
	 * transaction block.
	 */
	use_implicit_block = (list_length(parsetree_list) > 1);

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach(parsetree_item, parsetree_list)
	{
		RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
		bool		snapshot_set = false;
		const char	*commandTagName;
		QueryCompletion qc;
		MemoryContext per_parsetree_context = NULL;
		List	   *querytree_list,
				   *plantree_list;
		Portal		portal;
		DestReceiver *receiver;
		int16		format;

		pgstat_report_query_id(0, true);

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).  Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		qc.commandTag = CreateCommandTag(parsetree->stmt);
		commandTagName = GetCommandTagName(qc.commandTag);

		set_ps_display(commandTagName);

		BeginCommand(qc.commandTag, dest);

		if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND &&
			CheckDebugDtmActionSqlCommandTag(commandTagName))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FAULT_INJECT),
					 errmsg("Raise ERROR for debug_dtm_action = %d, commandTag = %s",
							Debug_dtm_action, commandTagName)));
		}

		/*
		 * GPDB: If we are connected in utility mode, disallow PREPARE
		 * TRANSACTION statements.
		 */
		if (Gp_role == GP_ROLE_UTILITY && IsA(parsetree->stmt, TransactionStmt) &&
			((TransactionStmt *) parsetree->stmt)->kind == TRANS_STMT_PREPARE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("PREPARE TRANSACTION is not supported in utility mode")));
		}

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(parsetree->stmt))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block"),
					 errdetail_abort()));

		/* Make sure we are in a transaction command */
		start_xact_command();

		/*
		 * If using an implicit transaction block, and we're not already in a
		 * transaction block, start an implicit block to force this statement
		 * to be grouped together with any following ones.  (We must do this
		 * each time through the loop; otherwise, a COMMIT/ROLLBACK in the
		 * list would cause later statements to not be grouped.)
		 */
		if (use_implicit_block)
			BeginImplicitTransactionBlock();

		/* If we got a cancel signal in parsing or prior command, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Set up a snapshot if parse analysis/planning will need one.
		 */
		if (analyze_requires_snapshot(parsetree))
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_set = true;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing query and plan trees
		 * (these can't be in the transaction context, as that will get reset
		 * when the command is COMMIT/ROLLBACK).  If we have multiple
		 * parsetrees, we use a separate context for each one, so that we can
		 * free that memory before moving on to the next one.  But for the
		 * last (or only) parsetree, just use MessageContext, which will be
		 * reset shortly after completion anyway.  In event of an error, the
		 * per_parsetree_context will be deleted when MessageContext is reset.
		 */
		if (lnext(parsetree_list, parsetree_item) != NULL)
		{
			per_parsetree_context =
				AllocSetContextCreate(MessageContext,
									  "per-parsetree message context",
									  ALLOCSET_DEFAULT_SIZES);
			oldcontext = MemoryContextSwitchTo(per_parsetree_context);
		}
		else
			oldcontext = MemoryContextSwitchTo(MessageContext);

		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0, NULL);

		plantree_list = pg_plan_queries(querytree_list, query_string,
										CURSOR_OPT_PARALLEL_OK, NULL);

		/*
		 * Done with the snapshot used for parsing/planning.
		 *
		 * While it looks promising to reuse the same snapshot for query
		 * execution (at least for simple protocol), unfortunately it causes
		 * execution to use a snapshot that has been acquired before locking
		 * any of the tables mentioned in the query.  This creates user-
		 * visible anomalies, so refrain.  Refer to
		 * https://postgr.es/m/flat/5075D8DF.6050500@fuzzy.cz for details.
		 */
		if (snapshot_set)
			PopActiveSnapshot();

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Create unnamed portal to run the query or queries in. If there
		 * already is one, silently drop it.
		 */
		portal = CreatePortal("", true, true);
		/* Don't display the portal in pg_cursors */
		portal->visible = false;

		/*
		 * We don't have to copy anything into the portal, because everything
		 * we are passing here is in MessageContext or the
		 * per_parsetree_context, and so will outlive the portal anyway.
		 */
		PortalDefineQuery(portal,
						  NULL,
						  query_string,
						  nodeTag(parsetree->stmt),
						  qc.commandTag,
						  plantree_list,
						  NULL);

		/*
		 * Start the portal.  No parameters here.
		 */
		PortalStart(portal, NULL, 0, InvalidSnapshot, NULL);

		/*
		 * Select the appropriate output format: text unless we are doing a
		 * FETCH from a binary cursor.  (Pretty grotty to have to do this here
		 * --- but it avoids grottiness in other places.  Ah, the joys of
		 * backward compatibility...)
		 */
		format = 0;				/* TEXT is default */
		if (IsA(parsetree->stmt, FetchStmt))
		{
			FetchStmt  *stmt = (FetchStmt *) parsetree->stmt;

			if (!stmt->ismove)
			{
				Portal		fportal = GetPortalByName(stmt->portalname);

				if (PortalIsValid(fportal) &&
					(fportal->cursorOptions & CURSOR_OPT_BINARY))
					format = 1; /* BINARY */
			}
		}
		PortalSetResultFormat(portal, 1, &format);

		/*
		 * Now we can create the destination receiver object.
		 */
		receiver = CreateDestReceiver(dest);
		if (dest == DestRemote)
			SetRemoteDestReceiverParams(receiver, portal);

		/*
		 * Switch back to transaction context for execution.
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Run the portal to completion, and then drop it (and the receiver).
		 */
		(void) PortalRun(portal,
						 FETCH_ALL,
						 true,	/* always top level */
						 true,
						 receiver,
						 receiver,
						 &qc);

		receiver->rDestroy(receiver);

		PortalDrop(portal, false);

		if (lnext(parsetree_list, parsetree_item) == NULL)
		{
			/*
			 * If this is the last parsetree of the query string, close down
			 * transaction statement before reporting command-complete.  This
			 * is so that any end-of-transaction errors are reported before
			 * the command-complete message is issued, to avoid confusing
			 * clients who will expect either a command-complete message or an
			 * error, not one and then the other.  Also, if we're using an
			 * implicit transaction block, we must close that out first.
			 */
			if (use_implicit_block)
				EndImplicitTransactionBlock();
			finish_xact_command();
		}
		else if (IsA(parsetree->stmt, TransactionStmt))
		{
			/*
			 * If this was a transaction control statement, commit it. We will
			 * start a new xact command for the next command.
			 */
			finish_xact_command();
		}
		else
		{
			/*
			 * We had better not see XACT_FLAGS_NEEDIMMEDIATECOMMIT set if
			 * we're not calling finish_xact_command().  (The implicit
			 * transaction block should have prevented it from getting set.)
			 */
			Assert(!(MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT));

			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 */
			CommandCounterIncrement();

			/*
			 * Disable statement timeout between queries of a multi-query
			 * string, so that the timeout applies separately to each query.
			 * (Our next loop iteration will start a fresh timeout.)
			 */
			disable_statement_timeout();
		}

		if (Debug_dtm_action == DEBUG_DTM_ACTION_FAIL_END_COMMAND &&
			CheckDebugDtmActionSqlCommandTag(commandTagName))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FAULT_INJECT),
					 errmsg("Raise ERROR for debug_dtm_action = %d, commandTag = %s",
							Debug_dtm_action, commandTagName)));
		}

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		EndCommand(&qc, dest, false);

		/* Now we may drop the per-parsetree context, if one was created. */
		if (per_parsetree_context)
			MemoryContextDelete(per_parsetree_context);
	}							/* end loop over parsetrees */

	/*
	 * Close down transaction statement, if one is open.  (This will only do
	 * something if the parsetree list was empty; otherwise the last loop
	 * iteration already did it.)
	 */
	finish_xact_command();

	/*
	 * If there were no parsetrees, return EmptyQueryResponse message.
	 */
	if (!parsetree_list)
		NullCommand(dest);

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(false)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  statement: %s",
							msec_str, query_string),
					 errhidestmt(true),
					 errdetail_execute(parsetree_list)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("QUERY STATISTICS");

	TRACE_POSTGRESQL_QUERY_DONE(query_string);

	debug_query_string = NULL;
}

/*
 * exec_parse_message
 *
 * Execute a "Parse" protocol message.
 */
static void
exec_parse_message(const char *query_string,	/* string to execute */
				   const char *stmt_name,	/* name for prepared stmt */
				   Oid *paramTypes, /* parameter types */
				   int numParams)	/* number of parameters */
{
	MemoryContext unnamed_stmt_context = NULL;
	MemoryContext oldcontext;
	List	   *parsetree_list;
	RawStmt    *raw_parse_tree;
	List	   *querytree_list;
	CachedPlanSource *psrc;
	bool		is_named;
	bool		save_log_statement_stats = log_statement_stats;
	char		msec_str[32];
	NodeTag		sourceTag = T_Query;

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;

	pgstat_report_activity(STATE_RUNNING, query_string);

	set_ps_display("PARSE");

	if (save_log_statement_stats)
		ResetUsage();

	ereport(DEBUG2,
			(errmsg_internal("parse %s: %s",
							 *stmt_name ? stmt_name : "<unnamed>",
							 query_string)));

	/*
	 * Start up a transaction command so we can run parse analysis etc. (Note
	 * that this will normally change current memory context.) Nothing happens
	 * if we are already in one.  This also arms the statement timeout if
	 * necessary.
	 */
	start_xact_command();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 *
	 * We have two strategies depending on whether the prepared statement is
	 * named or not.  For a named prepared statement, we do parsing in
	 * MessageContext and copy the finished trees into the prepared
	 * statement's plancache entry; then the reset of MessageContext releases
	 * temporary space used by parsing and rewriting. For an unnamed prepared
	 * statement, we assume the statement isn't going to hang around long, so
	 * getting rid of temp space quickly is probably not worth the costs of
	 * copying parse trees.  So in this case, we create the plancache entry's
	 * query_context here, and do all the parsing work therein.
	 */
	is_named = (stmt_name[0] != '\0');
	if (is_named)
	{
		/* Named prepared statement --- parse in MessageContext */
		oldcontext = MemoryContextSwitchTo(MessageContext);
	}
	else
	{
		/* Unnamed prepared statement --- release any prior unnamed stmt */
		drop_unnamed_stmt();
		/* Create context for parsing */
		unnamed_stmt_context =
			AllocSetContextCreate(MessageContext,
								  "unnamed prepared statement",
								  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
	}

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);

	/*
	 * We only allow a single user statement in a prepared statement. This is
	 * mainly to keep the protocol simple --- otherwise we'd need to worry
	 * about multiple result tupdescs and things like that.
	 */
	if (list_length(parsetree_list) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot insert multiple commands into a prepared statement")));

	if (parsetree_list != NIL)
	{
		Query	   *query;
		bool		snapshot_set = false;

		raw_parse_tree = linitial_node(RawStmt, parsetree_list);

		if (IsA(raw_parse_tree->stmt, SelectStmt))
		{
			/*
			 * For extended query protocol, we cannot optimize to avoid
			 * ExclusiveLock in case of select-for-update and similar queries.
			 * If the subsequent 'E' message requests only a specific number
			 * of rows to be fetched, the command must be executed like a
			 * cursor and LockRows plan node cannot be executed within a
			 * reader gang (cursors in Cloudberry must be executed by a reader gang).
			 * For details please refer the mailing list:
			 * https://groups.google.com/a/greenplum.org/forum/#!msg/gpdb-dev/ugsZca1qLXU/CtUmzEa7CAAJ
			 *
			 * For single node, we should not disable the locking optimization because single node
			 * works like PostgreSQL.
			 */
			if (IS_SINGLENODE())
				((SelectStmt *)raw_parse_tree->stmt)->disableLockingOptimization = false;
			else if (enableLockOptimization)
			{
				SelectStmt *stmt = (SelectStmt *)raw_parse_tree->stmt;

				/*
				 * If GUC 'enableLockOptimization' is on, we try to optimize the lock for
				 * select-for-update and similar queries.
				 *
				 * If the lock cannot be optimized, use the default way.
				 */
				stmt->disableLockingOptimization = false;
				if (!checkCanOptSelectLockingClause(stmt))
				{
					((SelectStmt *)raw_parse_tree->stmt)->disableLockingOptimization = true;
				}
			}
			else
				((SelectStmt *)raw_parse_tree->stmt)->disableLockingOptimization = true;
		}

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ROLLBACK.  It is important that this test occur before we
		 * try to do parse analysis, rewrite, or planning, since all those
		 * phases try to do database accesses, which may fail in abort state.
		 * (It might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(raw_parse_tree->stmt))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block"),
					 errdetail_abort()));

		/*
		 * Create the CachedPlanSource before we do parse analysis, since it
		 * needs to see the unmodified raw parse tree.
		 */
		psrc = CreateCachedPlan(raw_parse_tree, query_string,
								CreateCommandTag(raw_parse_tree->stmt));

		/*
		 * Set up a snapshot if parse analysis will need one.
		 */
		if (analyze_requires_snapshot(raw_parse_tree))
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_set = true;
		}

		/*
		 * Analyze and rewrite the query.  Note that the originally specified
		 * parameter set is not required to be complete, so we have to use
		 * parse_analyze_varparams().
		 */
		if (log_parser_stats)
			ResetUsage();

		query = parse_analyze_varparams(raw_parse_tree,
										query_string,
										&paramTypes,
										&numParams);

		/*
		 * Check all parameter types got determined.
		 */
		for (int i = 0; i < numParams; i++)
		{
			Oid			ptype = paramTypes[i];

			if (ptype == InvalidOid || ptype == UNKNOWNOID)
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_DATATYPE),
						 errmsg("could not determine data type of parameter $%d",
								i + 1)));
		}

		if (log_parser_stats)
			ShowUsage("PARSE ANALYSIS STATISTICS");

		querytree_list = pg_rewrite_query(query);

		if (parsetree_list)
		{
			Node	   *parsetree = (Node *) linitial(parsetree_list);
			sourceTag = IsA(parsetree, RawStmt) ? nodeTag(((RawStmt *)parsetree)->stmt) : nodeTag(parsetree);
		}

		/* Done with the snapshot used for parsing */
		if (snapshot_set)
			PopActiveSnapshot();
	}
	else
	{
		/* Empty input string.  This is legal. */
		raw_parse_tree = NULL;
		psrc = CreateCachedPlan(raw_parse_tree, query_string,
								CMDTAG_UNKNOWN);
		querytree_list = NIL;
	}

	/*
	 * CachedPlanSource must be a direct child of MessageContext before we
	 * reparent unnamed_stmt_context under it, else we have a disconnected
	 * circular subgraph.  Klugy, but less so than flipping contexts even more
	 * above.
	 */
	if (unnamed_stmt_context)
		MemoryContextSetParent(psrc->context, MessageContext);

	/* Finish filling in the CachedPlanSource */
	CompleteCachedPlan(psrc,
					   querytree_list,
					   unnamed_stmt_context,
					   sourceTag,
					   paramTypes,
					   numParams,
					   NULL,
					   NULL,
					   CURSOR_OPT_PARALLEL_OK,	/* allow parallel mode */
					   true);	/* fixed result */

	/* If we got a cancel signal during analysis, quit */
	CHECK_FOR_INTERRUPTS();

	if (is_named)
	{
		/*
		 * Store the query as a prepared statement.
		 */
		StorePreparedStatement(stmt_name, psrc, false);
	}
	else
	{
		/*
		 * We just save the CachedPlanSource into unnamed_stmt_psrc.
		 */
		SaveCachedPlan(psrc);
		unnamed_stmt_psrc = psrc;
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * We do NOT close the open transaction command here; that only happens
	 * when the client sends Sync.  Instead, do CommandCounterIncrement just
	 * in case something happened during parse/plan.
	 */
	CommandCounterIncrement();

	/*
	 * Send ParseComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('1');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  parse %s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							query_string),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("PARSE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_bind_message
 *
 * Process a "Bind" message to create a portal from a prepared statement
 */
static void
exec_bind_message(StringInfo input_message)
{
	const char *portal_name;
	const char *stmt_name;
	int			numPFormats;
	int16	   *pformats = NULL;
	int			numParams;
	int			numRFormats;
	int16	   *rformats = NULL;
	CachedPlanSource *psrc;
	CachedPlan *cplan;
	Portal		portal;
	char	   *query_string;
	char	   *saved_stmt_name;
	ParamListInfo params;
	MemoryContext oldContext;
	bool		save_log_statement_stats = log_statement_stats;
	bool		snapshot_set = false;
	char		msec_str[32];
	ParamsErrorCbData params_data;
	ErrorContextCallback params_errcxt;

	/* Get the fixed part of the message */
	portal_name = pq_getmsgstring(input_message);
	stmt_name = pq_getmsgstring(input_message);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "Bind: portal %s stmt_name %s", portal_name, stmt_name);

	ereport(DEBUG2,
			(errmsg_internal("bind %s to %s",
							 *portal_name ? portal_name : "<unnamed>",
							 *stmt_name ? stmt_name : "<unnamed>")));

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
	{
		PreparedStatement *pstmt;

		pstmt = FetchPreparedStatement(stmt_name, true);
		psrc = pstmt->plansource;
	}
	else
	{
		/* special-case the unnamed statement */
		psrc = unnamed_stmt_psrc;
		if (!psrc)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = psrc->query_string;

	pgstat_report_activity(STATE_RUNNING, psrc->query_string);

	set_ps_display("BIND");

	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command so we can call functions etc. (Note that
	 * this will normally change current memory context.) Nothing happens if
	 * we are already in one.  This also arms the statement timeout if
	 * necessary.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Get the parameter format codes */
	numPFormats = pq_getmsgint(input_message, 2);
	if (numPFormats > 0)
	{
		pformats = (int16 *) palloc(numPFormats * sizeof(int16));
		for (int i = 0; i < numPFormats; i++)
			pformats[i] = pq_getmsgint(input_message, 2);
	}

	/* Get the parameter value count */
	numParams = pq_getmsgint(input_message, 2);

	if (numPFormats > 1 && numPFormats != numParams)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("bind message has %d parameter formats but %d parameters",
						numPFormats, numParams)));

	if (numParams != psrc->num_params)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
						numParams, stmt_name, psrc->num_params)));

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands. We
	 * disallow binding anything else to avoid problems with infrastructure
	 * that expects to run inside a valid transaction.  We also disallow
	 * binding any parameters, since we can't risk calling user-defined I/O
	 * functions.
	 */
	if (IsAbortedTransactionBlockState() &&
		(!(psrc->raw_parse_tree &&
		   IsTransactionExitStmt(psrc->raw_parse_tree->stmt)) ||
		 numParams != 0))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	/*
	 * Create the portal.  Allow silent replacement of an existing portal only
	 * if the unnamed portal is specified.
	 */
	if (portal_name[0] == '\0')
		portal = CreatePortal(portal_name, true, true);
	else
		portal = CreatePortal(portal_name, false, false);

	portal->is_extended_query = true;

	/*
	 * If GUC 'enableLockOptimization' is on, we try to optimize the lock for
	 * select-for-update and similar queries.
	 *
	 * If the lock for query can be optimized, we use tuple lock instead of
	 * relation lock to improve Concurrency Performance. LockRows is executed
	 * on segment, but reader gangs cannot execute LockRows, so we need to
	 * dispatch query plan to writer gangs, that's why we reset is_extended_query
	 * to false here.
	 */
	if (enableLockOptimization && IsA(psrc->raw_parse_tree->stmt, SelectStmt))
	{
		SelectStmt *stmt = (SelectStmt *)psrc->raw_parse_tree->stmt;

		if (checkCanOptSelectLockingClause(stmt))
		{
			portal->is_extended_query = false;
		}
	}

	/*
	 * Prepare to copy stuff into the portal's memory context.  We do all this
	 * copying first, because it could possibly fail (out-of-memory) and we
	 * don't want a failure to occur between GetCachedPlan and
	 * PortalDefineQuery; that would result in leaking our plancache refcount.
	 */
	oldContext = MemoryContextSwitchTo(portal->portalContext);

	/* Copy the plan's query string into the portal */
	query_string = pstrdup(psrc->query_string);

	/* Likewise make a copy of the statement name, unless it's unnamed */
	if (stmt_name[0])
		saved_stmt_name = pstrdup(stmt_name);
	else
		saved_stmt_name = NULL;

	/*
	 * Set a snapshot if we have parameters to fetch (since the input
	 * functions might need it) or the query isn't a utility command (and
	 * hence could require redoing parse analysis and planning).  We keep the
	 * snapshot active till we're done, so that plancache.c doesn't have to
	 * take new ones.
	 */
	if (numParams > 0 ||
		(psrc->raw_parse_tree &&
		 analyze_requires_snapshot(psrc->raw_parse_tree)))
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		snapshot_set = true;
	}

	/*
	 * Fetch parameters, if any, and store in the portal's memory context.
	 */
	if (numParams > 0)
	{
		char	  **knownTextValues = NULL; /* allocate on first use */
		BindParamCbData one_param_data;

		/*
		 * Set up an error callback so that if there's an error in this phase,
		 * we can report the specific parameter causing the problem.
		 */
		one_param_data.portalName = portal->name;
		one_param_data.paramno = -1;
		one_param_data.paramval = NULL;
		params_errcxt.previous = error_context_stack;
		params_errcxt.callback = bind_param_error_callback;
		params_errcxt.arg = (void *) &one_param_data;
		error_context_stack = &params_errcxt;

		params = makeParamList(numParams);

		for (int paramno = 0; paramno < numParams; paramno++)
		{
			Oid			ptype = psrc->param_types[paramno];
			int32		plength;
			Datum		pval;
			bool		isNull;
			StringInfoData pbuf;
			char		csave;
			int16		pformat;

			one_param_data.paramno = paramno;
			one_param_data.paramval = NULL;

			plength = pq_getmsgint(input_message, 4);
			isNull = (plength == -1);

			if (!isNull)
			{
				const char *pvalue = pq_getmsgbytes(input_message, plength);

				/*
				 * Rather than copying data around, we just set up a phony
				 * StringInfo pointing to the correct portion of the message
				 * buffer.  We assume we can scribble on the message buffer so
				 * as to maintain the convention that StringInfos have a
				 * trailing null.  This is grotty but is a big win when
				 * dealing with very large parameter strings.
				 */
				pbuf.data = unconstify(char *, pvalue);
				pbuf.maxlen = plength + 1;
				pbuf.len = plength;
				pbuf.cursor = 0;

				csave = pbuf.data[plength];
				pbuf.data[plength] = '\0';
			}
			else
			{
				pbuf.data = NULL;	/* keep compiler quiet */
				csave = 0;
			}

			if (numPFormats > 1)
				pformat = pformats[paramno];
			else if (numPFormats > 0)
				pformat = pformats[0];
			else
				pformat = 0;	/* default = text */

			if (pformat == 0)	/* text mode */
			{
				Oid			typinput;
				Oid			typioparam;
				char	   *pstring;

				getTypeInputInfo(ptype, &typinput, &typioparam);

				/*
				 * We have to do encoding conversion before calling the
				 * typinput routine.
				 */
				if (isNull)
					pstring = NULL;
				else
					pstring = pg_client_to_server(pbuf.data, plength);

				/* Now we can log the input string in case of error */
				one_param_data.paramval = pstring;

				pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

				one_param_data.paramval = NULL;

				/*
				 * If we might need to log parameters later, save a copy of
				 * the converted string in MessageContext; then free the
				 * result of encoding conversion, if any was done.
				 */
				if (pstring)
				{
					if (log_parameter_max_length_on_error != 0)
					{
						MemoryContext oldcxt;

						oldcxt = MemoryContextSwitchTo(MessageContext);

						if (knownTextValues == NULL)
							knownTextValues =
								palloc0(numParams * sizeof(char *));

						if (log_parameter_max_length_on_error < 0)
							knownTextValues[paramno] = pstrdup(pstring);
						else
						{
							/*
							 * We can trim the saved string, knowing that we
							 * won't print all of it.  But we must copy at
							 * least two more full characters than
							 * BuildParamLogString wants to use; otherwise it
							 * might fail to include the trailing ellipsis.
							 */
							knownTextValues[paramno] =
								pnstrdup(pstring,
										 log_parameter_max_length_on_error
										 + 2 * MAX_MULTIBYTE_CHAR_LEN);
						}

						MemoryContextSwitchTo(oldcxt);
					}
					if (pstring != pbuf.data)
						pfree(pstring);
				}
			}
			else if (pformat == 1)	/* binary mode */
			{
				Oid			typreceive;
				Oid			typioparam;
				StringInfo	bufptr;

				/*
				 * Call the parameter type's binary input converter
				 */
				getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

				if (isNull)
					bufptr = NULL;
				else
					bufptr = &pbuf;

				pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

				/* Trouble if it didn't eat the whole buffer */
				if (!isNull && pbuf.cursor != pbuf.len)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
							 errmsg("incorrect binary data format in bind parameter %d",
									paramno + 1)));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unsupported format code: %d",
								pformat)));
				pval = 0;		/* keep compiler quiet */
			}

			/* Restore message buffer contents */
			if (!isNull)
				pbuf.data[plength] = csave;

			params->params[paramno].value = pval;
			params->params[paramno].isnull = isNull;

			/*
			 * We mark the params as CONST.  This ensures that any custom plan
			 * makes full use of the parameter values.
			 */
			params->params[paramno].pflags = PARAM_FLAG_CONST;
			params->params[paramno].ptype = ptype;
		}

		/* Pop the per-parameter error callback */
		error_context_stack = error_context_stack->previous;

		/*
		 * Once all parameters have been received, prepare for printing them
		 * in future errors, if configured to do so.  (This is saved in the
		 * portal, so that they'll appear when the query is executed later.)
		 */
		if (log_parameter_max_length_on_error != 0)
			params->paramValuesStr =
				BuildParamLogString(params,
									knownTextValues,
									log_parameter_max_length_on_error);
	}
	else
		params = NULL;

	/* Done storing stuff in portal's context */
	MemoryContextSwitchTo(oldContext);

	/*
	 * Set up another error callback so that all the parameters are logged if
	 * we get an error during the rest of the BIND processing.
	 */
	params_data.portalName = portal->name;
	params_data.params = params;
	params_errcxt.previous = error_context_stack;
	params_errcxt.callback = ParamsErrorCallback;
	params_errcxt.arg = (void *) &params_data;
	error_context_stack = &params_errcxt;

	/* Get the result format codes */
	numRFormats = pq_getmsgint(input_message, 2);
	if (numRFormats > 0)
	{
		rformats = (int16 *) palloc(numRFormats * sizeof(int16));
		for (int i = 0; i < numRFormats; i++)
			rformats[i] = pq_getmsgint(input_message, 2);
	}

	pq_getmsgend(input_message);

	/*
	 * Obtain a plan from the CachedPlanSource.  Any cruft from (re)planning
	 * will be generated in MessageContext.  The plan refcount will be
	 * assigned to the Portal, so it will be released at portal destruction.
	 */
	cplan = GetCachedPlan(psrc, params, NULL, NULL, NULL);

	/*
	 * Now we can define the portal.
	 *
	 * DO NOT put any code that could possibly throw an error between the
	 * above GetCachedPlan call and here.
	 */
	PortalDefineQuery(portal,
					  saved_stmt_name,
					  query_string,
					  psrc->sourceTag,
					  psrc->commandTag,
					  cplan->stmt_list,
					  cplan);

	/* Done with the snapshot used for parameter I/O and parsing/planning */
	if (snapshot_set)
		PopActiveSnapshot();

	/*
	 * And we're ready to start portal execution.
	 */
	PortalStart(portal, params, 0, InvalidSnapshot, NULL);

	/*
	 * Apply the result format requests to the portal.
	 */
	PortalSetResultFormat(portal, numRFormats, rformats);

	/*
	 * Done binding; remove the parameters error callback.  Entries emitted
	 * later determine independently whether to log the parameters or not.
	 */
	error_context_stack = error_context_stack->previous;

	/*
	 * Send BindComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('2');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  bind %s%s%s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							psrc->query_string),
					 errhidestmt(true),
					 errdetail_params(params)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("BIND MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_execute_message
 *
 * Process an "Execute" message for a portal
 */
static void
exec_execute_message(const char *portal_name, int64 max_rows)
{
	CommandDest dest;
	DestReceiver *receiver;
	Portal		portal;
	bool		completed;
	QueryCompletion qc;
	const char *sourceText;
	const char *prepStmtName;
	ParamListInfo portalParams;
	bool		save_log_statement_stats = log_statement_stats;
	bool		is_xact_command;
	bool		execute_is_fetch;
	bool		was_logged = false;
	char		msec_str[32];
	ParamsErrorCbData params_data;
	ErrorContextCallback params_errcxt;

	/* Adjust destination to tell printtup.c what to do */
	dest = whereToSendOutput;
	if (dest == DestRemote)
		dest = DestRemoteExecute;

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist", portal_name)));

	/*
	 * If the original query was a null string, just return
	 * EmptyQueryResponse.
	 */
	if (portal->commandTag == CMDTAG_UNKNOWN)
	{
		Assert(portal->stmts == NIL);
		NullCommand(dest);
		return;
	}

	if (Gp_role != GP_ROLE_EXECUTE)
	{

		/*
		 * MPP-20924
		 * Increment command_count only if we're executing utility statements
		 * In all other cases, we already incremented it in CreateQueryDescr
		 */
		bool is_utility_stmt = true;
		ListCell   *stmtlist_item = NULL;
		foreach(stmtlist_item, portal->stmts)
		{
			Node *stmt = lfirst(stmtlist_item);
			if (IsA(stmt, PlannedStmt))
			{
				is_utility_stmt = false;
				break;
			}
		}
		if (is_utility_stmt)
			increment_command_count();
	}

	/* Does the portal contain a transaction command? */
	is_xact_command = IsTransactionStmtList(portal->stmts);

	/*
	 * We must copy the sourceText and prepStmtName into MessageContext in
	 * case the portal is destroyed during finish_xact_command.  We do not
	 * make a copy of the portalParams though, preferring to just not print
	 * them in that case.
	 */
	sourceText = pstrdup(portal->sourceText);
	if (portal->prepStmtName)
		prepStmtName = pstrdup(portal->prepStmtName);
	else
		prepStmtName = "<unnamed>";
	portalParams = portal->portalParams;

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = sourceText;

	pgstat_report_activity(STATE_RUNNING, sourceText);

	set_ps_display(GetCommandTagName(portal->commandTag));

	if (save_log_statement_stats)
		ResetUsage();

	BeginCommand(portal->commandTag, dest);

	/*
	 * Create dest receiver in MessageContext (we don't want it in transaction
	 * context, because that may get deleted if portal contains VACUUM).
	 */
	receiver = CreateDestReceiver(dest);
	if (dest == DestRemoteExecute)
		SetRemoteDestReceiverParams(receiver, portal);

	/*
	 * Ensure we are in a transaction command (this should normally be the
	 * case already due to prior BIND).
	 */
	start_xact_command();

	/*
	 * If we re-issue an Execute protocol request against an existing portal,
	 * then we are only fetching more rows rather than completely re-executing
	 * the query from the start. atStart is never reset for a v3 portal, so we
	 * are safe to use this check.
	 */
	execute_is_fetch = !portal->atStart;

	/* Log immediately if dictated by log_statement */
	if (check_log_statement(portal->stmts))
	{
		ereport(LOG,
				(errmsg("%s %s%s%s: %s",
						execute_is_fetch ?
						_("execute fetch from") :
						_("execute"),
						prepStmtName,
						*portal_name ? "/" : "",
						*portal_name ? portal_name : "",
						sourceText),
				 errhidestmt(true),
				 errdetail_params(portalParams)));
		was_logged = true;
	}

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands.
	 */
	if (IsAbortedTransactionBlockState() &&
		!IsTransactionExitStmtList(portal->stmts))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	/* Check for cancel signal before we start execution */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Okay to run the portal.  Set the error callback so that parameters are
	 * logged.  The parameters must have been saved during the bind phase.
	 */
	params_data.portalName = portal->name;
	params_data.params = portalParams;
	params_errcxt.previous = error_context_stack;
	params_errcxt.callback = ParamsErrorCallback;
	params_errcxt.arg = (void *) &params_data;
	error_context_stack = &params_errcxt;

	if (max_rows <= 0)
		max_rows = FETCH_ALL;

	/*
	 * If the lock for select-for-update and similar queries is optimized, we should
	 * fetch all rows here.
	 *
	 * Since we optimize the lock for query, the query plan is dispatched to writer
	 * gangs to execute. If we do not fetch all rows here, the writer gangs are occupied
	 * and can not execute other query plans.
	 */
	if (enableLockOptimization)
	{
		CachedPlanSource *psrc;

		if (portal->prepStmtName)
		{
			PreparedStatement *pstmt;

			pstmt = FetchPreparedStatement(portal->prepStmtName, true);
			psrc = pstmt->plansource;
		}
		else
		{
			/* special-case the unnamed statement */
			psrc = unnamed_stmt_psrc;
			if (!psrc)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
						errmsg("unnamed prepared statement does not exist")));
		}

		if (IsA(psrc->raw_parse_tree->stmt, SelectStmt) &&
			checkCanOptSelectLockingClause((SelectStmt *)psrc->raw_parse_tree->stmt) &&
			max_rows != FETCH_ALL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Should fetch all rows for query if lock optimization is enabled")));
		}
	}

	completed = PortalRun(portal,
						  max_rows,
						  true, /* always top level */
						  !execute_is_fetch && max_rows == FETCH_ALL,
						  receiver,
						  receiver,
						  &qc);

	receiver->rDestroy(receiver);

	/* Done executing; remove the params error callback */
	error_context_stack = error_context_stack->previous;

	if (completed)
	{
		if (is_xact_command || (MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT))
		{
			/*
			 * If this was a transaction control statement, commit it.  We
			 * will start a new xact command for the next command (if any).
			 * Likewise if the statement required immediate commit.  Without
			 * this provision, we wouldn't force commit until Sync is
			 * received, which creates a hazard if the client tries to
			 * pipeline immediate-commit statements.
			 */
			finish_xact_command();

			/*
			 * These commands typically don't have any parameters, and even if
			 * one did we couldn't print them now because the storage went
			 * away during finish_xact_command.  So pretend there were none.
			 */
			portalParams = NULL;
		}
		else
		{
			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 */
			CommandCounterIncrement();

			/*
			 * Disable statement timeout whenever we complete an Execute
			 * message.  The next protocol message will start a fresh timeout.
			 */
			disable_statement_timeout();
		}

		/* Send appropriate CommandComplete to client */
		EndCommand(&qc, dest, false);
	}
	else
	{
		/* Portal run not complete, so send PortalSuspended */
		if (whereToSendOutput == DestRemote)
			pq_putemptymessage('s');
	}

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(false)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  %s %s%s%s: %s",
							msec_str,
							execute_is_fetch ?
							_("execute fetch from") :
							_("execute"),
							prepStmtName,
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							sourceText),
					 errhidestmt(true),
					 errdetail_params(portalParams)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("EXECUTE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * check_log_statement
 *		Determine whether command should be logged because of log_statement
 *
 * stmt_list can be either raw grammar output or a list of planned
 * statements
 */
static bool
check_log_statement(List *stmt_list)
{
	ListCell   *stmt_item;

	if (log_statement == LOGSTMT_NONE)
		return false;
	if (log_statement == LOGSTMT_ALL)
		return true;

	/* Else we have to inspect the statement(s) to see whether to log */
	foreach(stmt_item, stmt_list)
	{
		Node	   *stmt = (Node *) lfirst(stmt_item);

		if (GetCommandLogLevel(stmt) <= log_statement)
			return true;
	}

	return false;
}

/*
 * check_log_duration
 *		Determine whether current command's duration should be logged
 *		We also check if this statement in this transaction must be logged
 *		(regardless of its duration).
 *
 * Returns:
 *		0 if no logging is needed
 *		1 if just the duration should be logged
 *		2 if duration and query details should be logged
 *
 * If logging is needed, the duration in msec is formatted into msec_str[],
 * which must be a 32-byte buffer.
 *
 * was_logged should be true if caller already logged query details (this
 * essentially prevents 2 from being returned).
 */
int
check_log_duration(char *msec_str, bool was_logged)
{
	if (log_duration || log_min_duration_sample >= 0 ||
		log_min_duration_statement >= 0 || xact_is_sampled)
	{
		long		secs;
		int			usecs;
		int			msecs;
		bool		exceeded_duration;
		bool		exceeded_sample_duration;
		bool		in_sample = false;

		TimestampDifference(GetCurrentStatementStartTimestamp(),
							GetCurrentTimestamp(),
							&secs, &usecs);
		msecs = usecs / 1000;

		/*
		 * This odd-looking test for log_min_duration_* being exceeded is
		 * designed to avoid integer overflow with very long durations: don't
		 * compute secs * 1000 until we've verified it will fit in int.
		 */
		exceeded_duration = (log_min_duration_statement == 0 ||
							 (log_min_duration_statement > 0 &&
							  (secs > log_min_duration_statement / 1000 ||
							   secs * 1000 + msecs >= log_min_duration_statement)));

		exceeded_sample_duration = (log_min_duration_sample == 0 ||
									(log_min_duration_sample > 0 &&
									 (secs > log_min_duration_sample / 1000 ||
									  secs * 1000 + msecs >= log_min_duration_sample)));

		/*
		 * Do not log if log_statement_sample_rate = 0. Log a sample if
		 * log_statement_sample_rate <= 1 and avoid unnecessary random() call
		 * if log_statement_sample_rate = 1.
		 */
		if (exceeded_sample_duration)
			in_sample = log_statement_sample_rate != 0 &&
				(log_statement_sample_rate == 1 ||
				 random() <= log_statement_sample_rate * MAX_RANDOM_VALUE);

		if (exceeded_duration || in_sample || log_duration || xact_is_sampled)
		{
			snprintf(msec_str, 32, "%ld.%03d",
					 secs * 1000 + msecs, usecs % 1000);
			if ((exceeded_duration || in_sample || xact_is_sampled) && !was_logged)
				return 2;
			else
				return 1;
		}
	}

	return 0;
}

/*
 * errdetail_execute
 *
 * Add an errdetail() line showing the query referenced by an EXECUTE, if any.
 * The argument is the raw parsetree list.
 */
static int
errdetail_execute(List *raw_parsetree_list)
{
	ListCell   *parsetree_item;

	foreach(parsetree_item, raw_parsetree_list)
	{
		RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);

		if (IsA(parsetree->stmt, ExecuteStmt))
		{
			ExecuteStmt *stmt = (ExecuteStmt *) parsetree->stmt;
			PreparedStatement *pstmt;

			pstmt = FetchPreparedStatement(stmt->name, false);
			if (pstmt)
			{
				errdetail("prepare: %s", pstmt->plansource->query_string);
				return 0;
			}
		}
	}

	return 0;
}

/*
 * errdetail_params
 *
 * Add an errdetail() line showing bind-parameter data, if available.
 * Note that this is only used for statement logging, so it is controlled
 * by log_parameter_max_length not log_parameter_max_length_on_error.
 */
static int
errdetail_params(ParamListInfo params)
{
	if (params && params->numParams > 0 && log_parameter_max_length != 0)
	{
		char	   *str;

		str = BuildParamLogString(params, NULL, log_parameter_max_length);
		if (str && str[0] != '\0')
			errdetail("parameters: %s", str);
	}

	return 0;
}

/*
 * errdetail_abort
 *
 * Add an errdetail() line showing abort reason, if any.
 */
static int
errdetail_abort(void)
{
	if (MyProc->recoveryConflictPending)
		errdetail("abort reason: recovery conflict");

	return 0;
}

/*
 * errdetail_recovery_conflict
 *
 * Add an errdetail() line showing conflict source.
 */
static int
errdetail_recovery_conflict(void)
{
	switch (RecoveryConflictReason)
	{
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			errdetail("User was holding shared buffer pin for too long.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			errdetail("User was holding a relation lock for too long.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			errdetail("User was or might have been using tablespace that must be dropped.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			errdetail("User query might have needed to see row versions that must be removed.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			errdetail("User transaction caused buffer deadlock with recovery.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:
			errdetail("User was connected to a database that must be dropped.");
			break;
		default:
			break;
			/* no errdetail */
	}

	return 0;
}

/*
 * bind_param_error_callback
 *
 * Error context callback used while parsing parameters in a Bind message
 */
static void
bind_param_error_callback(void *arg)
{
	BindParamCbData *data = (BindParamCbData *) arg;
	StringInfoData buf;
	char	   *quotedval;

	if (data->paramno < 0)
		return;

	/* If we have a textual value, quote it, and trim if necessary */
	if (data->paramval)
	{
		initStringInfo(&buf);
		appendStringInfoStringQuoted(&buf, data->paramval,
									 log_parameter_max_length_on_error);
		quotedval = buf.data;
	}
	else
		quotedval = NULL;

	if (data->portalName && data->portalName[0] != '\0')
	{
		if (quotedval)
			errcontext("portal \"%s\" parameter $%d = %s",
					   data->portalName, data->paramno + 1, quotedval);
		else
			errcontext("portal \"%s\" parameter $%d",
					   data->portalName, data->paramno + 1);
	}
	else
	{
		if (quotedval)
			errcontext("unnamed portal parameter $%d = %s",
					   data->paramno + 1, quotedval);
		else
			errcontext("unnamed portal parameter $%d",
					   data->paramno + 1);
	}

	if (quotedval)
		pfree(quotedval);
}

/*
 * exec_describe_statement_message
 *
 * Process a "Describe" message for a prepared statement
 */
static void
exec_describe_statement_message(const char *stmt_name)
{
	CachedPlanSource *psrc;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
	{
		PreparedStatement *pstmt;

		pstmt = FetchPreparedStatement(stmt_name, true);
		psrc = pstmt->plansource;
	}
	else
	{
		/* special-case the unnamed statement */
		psrc = unnamed_stmt_psrc;
		if (!psrc)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/* Prepared statements shouldn't have changeable result descs */
	Assert(psrc->fixed_result);

	/*
	 * If we are in aborted transaction state, we can't run
	 * SendRowDescriptionMessage(), because that needs catalog accesses.
	 * Hence, refuse to Describe statements that return data.  (We shouldn't
	 * just refuse all Describes, since that might break the ability of some
	 * clients to issue COMMIT or ROLLBACK commands, if they use code that
	 * blindly Describes whatever it does.)  We can Describe parameters
	 * without doing anything dangerous, so we don't restrict that.
	 */
	if (IsAbortedTransactionBlockState() &&
		psrc->resultDesc)
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	/*
	 * First describe the parameters...
	 */
	pq_beginmessage_reuse(&row_description_buf, 't');	/* parameter description
														 * message type */
	pq_sendint16(&row_description_buf, psrc->num_params);

	for (int i = 0; i < psrc->num_params; i++)
	{
		Oid			ptype = psrc->param_types[i];

		pq_sendint32(&row_description_buf, (int) ptype);
	}
	pq_endmessage_reuse(&row_description_buf);

	/*
	 * Next send RowDescription or NoData to describe the result...
	 */
	if (psrc->resultDesc)
	{
		List	   *tlist;

		/* Get the plan's primary targetlist */
		tlist = CachedPlanGetTargetList(psrc, NULL);

		SendRowDescriptionMessage(&row_description_buf,
								  psrc->resultDesc,
								  tlist,
								  NULL);
	}
	else
		pq_putemptymessage('n');	/* NoData */

}

/*
 * exec_describe_portal_message
 *
 * Process a "Describe" message for a portal
 */
static void
exec_describe_portal_message(const char *portal_name)
{
	Portal		portal;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist", portal_name)));

	/*
	 * If we are in aborted transaction state, we can't run
	 * SendRowDescriptionMessage(), because that needs catalog accesses.
	 * Hence, refuse to Describe portals that return data.  (We shouldn't just
	 * refuse all Describes, since that might break the ability of some
	 * clients to issue COMMIT or ROLLBACK commands, if they use code that
	 * blindly Describes whatever it does.)
	 */
	if (IsAbortedTransactionBlockState() &&
		portal->tupDesc)
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	if (portal->tupDesc)
		SendRowDescriptionMessage(&row_description_buf,
								  portal->tupDesc,
								  FetchPortalTargetList(portal),
								  portal->formats);
	else
		pq_putemptymessage('n');	/* NoData */
}


/*
 * Convenience routines for starting/committing a single command.
 */
static void
start_xact_command(void)
{
	if (!xact_started)
	{
		StartTransactionCommand();

		xact_started = true;
	}

	/*
	 * Start statement timeout if necessary.  Note that this'll intentionally
	 * not reset the clock on an already started timeout, to avoid the timing
	 * overhead when start_xact_command() is invoked repeatedly, without an
	 * interceding finish_xact_command() (e.g. parse/bind/execute).  If that's
	 * not desired, the timeout has to be disabled explicitly.
	 */
	enable_statement_timeout();

	/* Start timeout for checking if the client has gone away if necessary. */
	if (client_connection_check_interval > 0 &&
		IsUnderPostmaster &&
		MyProcPort &&
		!get_timeout_active(CLIENT_CONNECTION_CHECK_TIMEOUT))
		enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT,
							 client_connection_check_interval);
}

static void
finish_xact_command(void)
{
	/* cancel active statement timeout after each command */
	disable_statement_timeout();

	if (xact_started)
	{
		CommitTransactionCommand();

#ifdef MEMORY_CONTEXT_CHECKING
		/* Check all memory contexts that weren't freed during commit */
		/* (those that were, were checked before being deleted) */
		MemoryContextCheck(TopMemoryContext);
#endif

#ifdef SHOW_MEMORY_STATS
		/* Print mem stats after each commit for leak tracking */
		MemoryContextStats(TopMemoryContext);
#endif

		xact_started = false;
	}
}


/*
 * Convenience routines for checking whether a statement is one of the
 * ones that we allow in transaction-aborted state.
 */

/* Test a bare parsetree */
static bool
IsTransactionExitStmt(Node *parsetree)
{
	if (parsetree && IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;

		if (stmt->kind == TRANS_STMT_COMMIT ||
			stmt->kind == TRANS_STMT_PREPARE ||
			stmt->kind == TRANS_STMT_ROLLBACK ||
			stmt->kind == TRANS_STMT_ROLLBACK_TO)
			return true;
	}
	return false;
}

/* Test a list that contains PlannedStmt nodes */
static bool
IsTransactionExitStmtList(List *pstmts)
{
	if (list_length(pstmts) == 1)
	{
		PlannedStmt *pstmt = linitial_node(PlannedStmt, pstmts);

		if (pstmt->commandType == CMD_UTILITY &&
			IsTransactionExitStmt(pstmt->utilityStmt))
			return true;
	}
	return false;
}

/* Test a list that contains PlannedStmt nodes */
static bool
IsTransactionStmtList(List *pstmts)
{
	if (list_length(pstmts) == 1)
	{
		PlannedStmt *pstmt = linitial_node(PlannedStmt, pstmts);

		if (pstmt->commandType == CMD_UTILITY &&
			IsA(pstmt->utilityStmt, TransactionStmt))
			return true;
	}
	return false;
}

/* Release any existing unnamed prepared statement */
static void
drop_unnamed_stmt(void)
{
	/* paranoia to avoid a dangling pointer in case of error */
	if (unnamed_stmt_psrc)
	{
		CachedPlanSource *psrc = unnamed_stmt_psrc;

		unnamed_stmt_psrc = NULL;
		DropCachedPlan(psrc);
	}
}


/* --------------------------------
 *		signal handler routines used in PostgresMain()
 * --------------------------------
 */

/*
 * quickdie() occurs when signaled SIGQUIT by the postmaster.
 *
 * Either some backend has bought the farm, or we've been told to shut down
 * "immediately"; so we need to stop what we're doing and exit.
 *
 * NOTE: see MPP-9518 and MPP-7564, there are other backend processes
 * which come through here, there isn't anything specific to any particular
 * backend here. If these other processes need to do their own handling
 * they should override the signal handler for SIGQUIT (and in that handler
 * call this one ?).
 *
 *
 * @param SIGNAL_ARGS -- so the signature matches a signal handler.  Nore that
 */
void
quickdie(SIGNAL_ARGS)
{
	sigaddset(&BlockSig, SIGQUIT);	/* prevent nested calls */
	PG_SETMASK(&BlockSig);

	in_quickdie=true;

	/*
	 * Prevent interrupts while exiting; though we just blocked signals that
	 * would queue new interrupts, one may have been pending.  We don't want a
	 * quickdie() downgraded to a mere query cancel.
	 */
	HOLD_INTERRUPTS();

	/*
	 * If we're aborting out of client auth, don't risk trying to send
	 * anything to the client; we will likely violate the protocol, not to
	 * mention that we may have interrupted the guts of OpenSSL or some
	 * authentication library.
	 */
	if (ClientAuthInProgress && whereToSendOutput == DestRemote)
		whereToSendOutput = DestNone;

	/*
	 * Notify the client before exiting, to give a clue on what happened.
	 *
	 * It's dubious to call ereport() from a signal handler.  It is certainly
	 * not async-signal safe.  But it seems better to try, than to disconnect
	 * abruptly and leave the client wondering what happened.  It's remotely
	 * possible that we crash or hang while trying to send the message, but
	 * receiving a SIGQUIT is a sign that something has already gone badly
	 * wrong, so there's not much to lose.  Assuming the postmaster is still
	 * running, it will SIGKILL us soon if we get stuck for some reason.
	 *
	 * One thing we can do to make this a tad safer is to clear the error
	 * context stack, so that context callbacks are not called.  That's a lot
	 * less code that could be reached here, and the context info is unlikely
	 * to be very relevant to a SIGQUIT report anyway.
	 */
	error_context_stack = NULL;

	/*
	 * When responding to a postmaster-issued signal, we send the message only
	 * to the client; sending to the server log just creates log spam, plus
	 * it's more code that we need to hope will work in a signal handler.
	 *
	 * Ideally these should be ereport(FATAL), but then we'd not get control
	 * back to force the correct type of process exit.
	 */
	switch (GetQuitSignalReason())
	{
		case PMQUIT_NOT_SENT:
			/* Hmm, SIGQUIT arrived out of the blue */
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection because of unexpected SIGQUIT signal")));
			break;
		case PMQUIT_FOR_CRASH:
			/* A crash-and-restart cycle is in progress */
			ereport(WARNING_CLIENT_ONLY,
					(errcode(ERRCODE_CRASH_SHUTDOWN),
					 errmsg("terminating connection because of crash of another server process"),
					 errdetail("The postmaster has commanded this server process to roll back"
							   " the current transaction and exit, because another"
							   " server process exited abnormally and possibly corrupted"
							   " shared memory."),
					 errhint("In a moment you should be able to reconnect to the"
							 " database and repeat your command.")));
			break;
		case PMQUIT_FOR_STOP:
			/* Immediate-mode stop */
			ereport(WARNING_CLIENT_ONLY,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to immediate shutdown command")));
			break;
	}

	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we do _exit(2) not _exit(0).  This is to force the postmaster into
	 * a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}

/*
 * Shutdown signal from postmaster: abort transaction and exit
 * at soonest convenient time
 */
void
die(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	/* for the statistics collector */
	pgStatSessionEndCause = DISCONNECT_KILLED;

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

	/*
	 * If we're in single user mode, we want to quit immediately - we can't
	 * rely on latches as they wouldn't work when stdin/stdout is a file.
	 * Rather ugly, but it's unlikely to be worthwhile to invest much more
	 * effort just for the benefit of single user mode.
	 */
	if (DoingCommandRead && whereToSendOutput != DestRemote)
		ProcessInterrupts(__FILE__, __LINE__);

	errno = save_errno;
}

/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time
 */
void
StatementCancelHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		QueryCancelPending = true;
		QueryCancelCleanup = true;

		if (cancel_pending_hook)
			(*cancel_pending_hook)();
	}

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}


/* CDB: Signal handler for program errors */
static void
CdbProgramErrorHandler(SIGNAL_ARGS)
{
    int			save_errno = errno;
    char       *pts = "process";

	if (!pthread_equal(main_tid, pthread_self()))
	{
#ifndef _WIN32
		write_stderr("\nUnexpected internal error: Master %d received signal %d in worker thread %lu (forwarding signal to main thread)\n\n",
					 MyProcPid, postgres_signal_arg, (unsigned long)pthread_self());
#else
		write_stderr("\nUnexpected internal error: Master %d received signal %d in worker thread %lu (forwarding signal to main thread)\n\n",
					 MyProcPid, postgres_signal_arg, (unsigned long)pthread_self().p);
#endif
		/* Only forward if the main thread isn't quick-dying. */
		if (!in_quickdie)
			pthread_kill(main_tid, postgres_signal_arg);

		/*
		 * Don't exit the thread when we reraise SEGV/BUS/ILL signals to the OS.
		 * This thread will die together with the main thread after the OS reraises
		 * the signal. This is to ensure that the dumped core file contains the call
		 * stack on this thread for later debugging.
		 */
		if (!(gp_reraise_signal &&
			  (postgres_signal_arg == SIGSEGV ||
			   postgres_signal_arg == SIGILL ||
			   postgres_signal_arg == SIGBUS)))
		{
			pthread_exit(NULL);
		}

		return;
	}


    if (Gp_role == GP_ROLE_DISPATCH)
        pts = "Master process";
    else if (Gp_role == GP_ROLE_EXECUTE)
        pts = "Segment process";
    else
        pts = "Process";

    errno = save_errno;
    StandardHandlerForSigillSigsegvSigbus_OnMainThread(pts, PASS_SIGNAL_ARGS);
}

/* signal handler for floating point exception */
void
FloatExceptionHandler(SIGNAL_ARGS)
{
	/* We're not returning, so no need to save errno */
	ereport(ERROR,
			(errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
			 errmsg("floating-point exception"),
			 errdetail("An invalid floating-point operation was signaled. "
					   "This probably means an out-of-range result or an "
					   "invalid operation, such as division by zero.")));
}

/*
 * RecoveryConflictInterrupt: out-of-line portion of recovery conflict
 * handling following receipt of SIGUSR1. Designed to be similar to die()
 * and StatementCancelHandler(). Called only by a normal user backend
 * that begins a transaction during recovery.
 */
void
RecoveryConflictInterrupt(ProcSignalReason reason)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		RecoveryConflictReason = reason;
		switch (reason)
		{
			case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:

				/*
				 * If we aren't waiting for a lock we can never deadlock.
				 */
				if (!IsWaitingForLock())
					return;

				/* Intentional fall through to check wait for pin */
				/* FALLTHROUGH */

			case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:

				/*
				 * If PROCSIG_RECOVERY_CONFLICT_BUFFERPIN is requested but we
				 * aren't blocking the Startup process there is nothing more
				 * to do.
				 *
				 * When PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK is
				 * requested, if we're waiting for locks and the startup
				 * process is not waiting for buffer pin (i.e., also waiting
				 * for locks), we set the flag so that ProcSleep() will check
				 * for deadlocks.
				 */
				if (!HoldingBufferPinThatDelaysRecovery())
				{
					if (reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK &&
						GetStartupBufferPinWaitBufId() < 0)
						CheckDeadLockAlert();
					return;
				}

				MyProc->recoveryConflictPending = true;

				/* Intentional fall through to error handling */
				/* FALLTHROUGH */

			case PROCSIG_RECOVERY_CONFLICT_LOCK:
			case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:

				/*
				 * If we aren't in a transaction any longer then ignore.
				 */
				if (!IsTransactionOrTransactionBlock())
					return;

				/*
				 * If we can abort just the current subtransaction then we are
				 * OK to throw an ERROR to resolve the conflict. Otherwise
				 * drop through to the FATAL case.
				 *
				 * XXX other times that we can throw just an ERROR *may* be
				 * PROCSIG_RECOVERY_CONFLICT_LOCK if no locks are held in
				 * parent transactions
				 *
				 * PROCSIG_RECOVERY_CONFLICT_SNAPSHOT if no snapshots are held
				 * by parent transactions and the transaction is not
				 * transaction-snapshot mode
				 *
				 * PROCSIG_RECOVERY_CONFLICT_TABLESPACE if no temp files or
				 * cursors open in parent transactions
				 */
				if (!IsSubTransaction())
				{
					/*
					 * If we already aborted then we no longer need to cancel.
					 * We do this here since we do not wish to ignore aborted
					 * subtransactions, which must cause FATAL, currently.
					 */
					if (IsAbortedTransactionBlockState())
						return;

					RecoveryConflictPending = true;
					QueryCancelPending = true;
					InterruptPending = true;
					break;
				}

				/* Intentional fall through to session cancel */
				/* FALLTHROUGH */

			case PROCSIG_RECOVERY_CONFLICT_DATABASE:
				RecoveryConflictPending = true;
				ProcDiePending = true;
				InterruptPending = true;
				break;

			default:
				elog(FATAL, "unrecognized conflict mode: %d",
					 (int) reason);
		}

		Assert(RecoveryConflictPending && (QueryCancelPending || ProcDiePending));

		/*
		 * All conflicts apart from database cause dynamic errors where the
		 * command or transaction can be retried at a later point with some
		 * potential for success. No need to reset this, since non-retryable
		 * conflict errors are currently FATAL.
		 */
		if (reason == PROCSIG_RECOVERY_CONFLICT_DATABASE)
			RecoveryConflictRetryable = false;
	}

	/*
	 * Set the process latch. This function essentially emulates signal
	 * handlers like die() and StatementCancelHandler() and it seems prudent
	 * to behave similarly as they do.
	 */
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * ProcessInterrupts: out-of-line portion of CHECK_FOR_INTERRUPTS() macro
 *
 * If an interrupt condition is pending, and it's safe to service it,
 * then clear the flag and accept the interrupt.  Called only when
 * InterruptPending is true.
 *
 * Note: if INTERRUPTS_CAN_BE_PROCESSED() is true, then ProcessInterrupts
 * is guaranteed to clear the InterruptPending flag before returning.
 * (This is not the same as guaranteeing that it's still clear when we
 * return; another interrupt could have arrived.  But we promise that
 * any pre-existing one will have been serviced.)
 *
 * Parameters filename and lineno contain the file name and the line number where
 * ProcessInterrupts was invoked, respectively.
 */
void
ProcessInterrupts(const char* filename, int lineno)
{
	/* OK to accept any interrupts now? */
	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return;
	InterruptPending = false;

	if (ProcDiePending)
	{
		ProcDiePending = false;
		QueryCancelPending = false; /* ProcDie trumps QueryCancel */
		LockErrorCleanup();
		/* As in quickdie, don't risk sending to client during auth */
		if (ClientAuthInProgress && whereToSendOutput == DestRemote)
			whereToSendOutput = DestNone;
		if (ClientAuthInProgress)
			ereport(FATAL,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling authentication due to timeout")));
		else if (IsAutoVacuumWorkerProcess())
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating autovacuum process due to administrator command")));
		else if (IsLogicalWorker())
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating logical replication worker due to administrator command")));
		else if (IsLogicalLauncher())
		{
			ereport(DEBUG1,
					(errmsg_internal("logical replication launcher shutting down")));

			/*
			 * The logical replication launcher can be stopped at any time.
			 * Use exit status 1 so the background worker is restarted.
			 */
			proc_exit(1);
		}
		else if (RecoveryConflictPending && RecoveryConflictRetryable)
		{
			pgstat_report_recovery_conflict(RecoveryConflictReason);
			ereport(FATAL,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("terminating connection due to conflict with recovery"),
					 errdetail_recovery_conflict()));
		}
		else if (RecoveryConflictPending)
		{
			/* Currently there is only one non-retryable recovery conflict */
			Assert(RecoveryConflictReason == PROCSIG_RECOVERY_CONFLICT_DATABASE);
			pgstat_report_recovery_conflict(RecoveryConflictReason);
			ereport(FATAL,
					(errcode(ERRCODE_DATABASE_DROPPED),
					 errmsg("terminating connection due to conflict with recovery"),
					 errdetail_recovery_conflict()));
		}
		else if (IsBackgroundWorker)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating background worker \"%s\" due to administrator command",
							MyBgworkerEntry->bgw_type)));
		else
		{
			if (HasCancelMessage())
			{
				char   *buffer = palloc0(MAX_CANCEL_MSG);

				GetCancelMessage(&buffer, MAX_CANCEL_MSG);
				ereport(FATAL,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
						 errmsg("terminating connection due to administrator command: \"%s\"",
						 buffer)));
			}
			else
				ereport(FATAL,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
						 errmsg("terminating connection due to administrator command")));
		}
	}

	if (CheckClientConnectionPending)
	{
		CheckClientConnectionPending = false;

		/*
		 * Check for lost connection and re-arm, if still configured, but not
		 * if we've arrived back at DoingCommandRead state.  We don't want to
		 * wake up idle sessions, and they already know how to detect lost
		 * connections.
		 */
		if (!DoingCommandRead && client_connection_check_interval > 0)
		{
			if (!pq_check_connection())
				ClientConnectionLost = true;
			else
				enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT,
									 client_connection_check_interval);
		}
	}

	if (ClientConnectionLost)
	{
		QueryCancelPending = false; /* lost connection trumps QueryCancel */
		LockErrorCleanup();
		/* don't send to client, we already know the connection to be dead. */
		whereToSendOutput = DestNone;
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection to client lost")));
	}

	/*
	 * If a recovery conflict happens while we are waiting for input from the
	 * client, the client is presumably just sitting idle in a transaction,
	 * preventing recovery from making progress.  Terminate the connection to
	 * dislodge it.
	 */
	if (RecoveryConflictPending && DoingCommandRead)
	{
		QueryCancelPending = false; /* this trumps QueryCancel */
		RecoveryConflictPending = false;
		LockErrorCleanup();
		pgstat_report_recovery_conflict(RecoveryConflictReason);
		ereport(FATAL,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("terminating connection due to conflict with recovery"),
				 errdetail_recovery_conflict(),
				 errhint("In a moment you should be able to reconnect to the"
						 " database and repeat your command.")));
	}

	/*
	 * Don't allow query cancel interrupts while reading input from the
	 * client, because we might lose sync in the FE/BE protocol.  (Die
	 * interrupts are OK, because we won't read any further messages from the
	 * client in that case.)
	 */
	if (QueryCancelPending && QueryCancelHoldoffCount != 0)
	{
		/*
		 * Re-arm InterruptPending so that we process the cancel request as
		 * soon as we're done reading the message.  (XXX this is seriously
		 * ugly: it complicates INTERRUPTS_CAN_BE_PROCESSED(), and it means we
		 * can't use that macro directly as the initial test in this function,
		 * meaning that this code also creates opportunities for other bugs to
		 * appear.)
		 */
		InterruptPending = true;
	}
	else if (QueryCancelPending)
	{
		bool		lock_timeout_occurred;
		bool		stmt_timeout_occurred;

		elog(LOG,"Process interrupt for 'query cancel pending' (%s:%d)", filename, lineno);

		QueryCancelPending = false;

		/*
		 * If LOCK_TIMEOUT and STATEMENT_TIMEOUT indicators are both set, we
		 * need to clear both, so always fetch both.
		 */
		lock_timeout_occurred = get_timeout_indicator(LOCK_TIMEOUT, true);
		stmt_timeout_occurred = get_timeout_indicator(STATEMENT_TIMEOUT, true);

		/*
		 * If both were set, we want to report whichever timeout completed
		 * earlier; this ensures consistent behavior if the machine is slow
		 * enough that the second timeout triggers before we get here.  A tie
		 * is arbitrarily broken in favor of reporting a lock timeout.
		 */
		if (lock_timeout_occurred && stmt_timeout_occurred &&
			get_timeout_finish_time(STATEMENT_TIMEOUT) < get_timeout_finish_time(LOCK_TIMEOUT))
			lock_timeout_occurred = false;	/* report stmt timeout */

		if (lock_timeout_occurred)
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("canceling statement due to lock timeout")));
		}
		if (stmt_timeout_occurred)
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling statement due to statement timeout")));
		}
		if (IsAutoVacuumWorkerProcess())
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling autovacuum task")));
		}
		if (RecoveryConflictPending)
		{
			RecoveryConflictPending = false;
			LockErrorCleanup();
			pgstat_report_recovery_conflict(RecoveryConflictReason);
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("canceling statement due to conflict with recovery"),
					 errdetail_recovery_conflict()));
		}

		/*
		 * If we are reading a command from the client, just ignore the cancel
		 * request --- sending an extra error message won't accomplish
		 * anything.  Otherwise, go ahead and throw the error.
		 */
		if (!DoingCommandRead)
		{
			StringInfoData cancel_msg_str;

			LockErrorCleanup();
			initStringInfo(&cancel_msg_str);

			if (HasCancelMessage())
			{
				char *buffer = palloc0(MAX_CANCEL_MSG);

				GetCancelMessage(&buffer, MAX_CANCEL_MSG);
				appendStringInfo(&cancel_msg_str, ": \"%s\"", buffer);
				pfree(buffer);
			}

			if (Gp_role == GP_ROLE_EXECUTE)
				ereport(ERROR,
						(errcode(ERRCODE_GP_OPERATION_CANCELED),
						 errmsg("canceling MPP operation%s", cancel_msg_str.data)));
			else
			{
				char		msec_str[32];

				switch (check_log_duration(msec_str, false))
				{
					case 0:
						ereport(ERROR,
								(errcode(ERRCODE_QUERY_CANCELED),
										errmsg("canceling statement due to user request%s", cancel_msg_str.data)));
						break;
					case 1:
					case 2:
						ereport(ERROR,
								(errcode(ERRCODE_QUERY_CANCELED),
										errmsg("canceling statement due to user request%s, duration:%s",
											   cancel_msg_str.data, msec_str)));
						break;
				}
			}
		}
	}

	if (IdleInTransactionSessionTimeoutPending)
	{
		/*
		 * If the GUC has been reset to zero, ignore the signal.  This is
		 * important because the GUC update itself won't disable any pending
		 * interrupt.
		 */
		if (IdleInTransactionSessionTimeout > 0)
			ereport(FATAL,
					(errcode(ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT),
					 errmsg("terminating connection due to idle-in-transaction timeout")));
		else
			IdleInTransactionSessionTimeoutPending = false;
	}

	if (IdleSessionTimeoutPending)
	{
		/* As above, ignore the signal if the GUC has been reset to zero. */
		if (IdleSessionTimeout > 0)
			ereport(FATAL,
					(errcode(ERRCODE_IDLE_SESSION_TIMEOUT),
					 errmsg("terminating connection due to idle-session timeout")));
		else
			IdleSessionTimeoutPending = false;
	}

	if (ProcSignalBarrierPending)
		ProcessProcSignalBarrier();

	if (ParallelMessagePending)
		HandleParallelMessages();

	if (LogMemoryContextPending)
		ProcessLogMemoryContextInterrupt();
}

/*
 * IA64-specific code to fetch the AR.BSP register for stack depth checks.
 *
 * We currently support gcc, icc, and HP-UX's native compiler here.
 *
 * Note: while icc accepts gcc asm blocks on x86[_64], this is not true on
 * ia64 (at least not in icc versions before 12.x).  So we have to carry a
 * separate implementation for it.
 */
#if defined(__ia64__) || defined(__ia64)

#if defined(__hpux) && !defined(__GNUC__) && !defined(__INTEL_COMPILER)
/* Assume it's HP-UX native compiler */
#include <ia64/sys/inline.h>
#define ia64_get_bsp() ((char *) (_Asm_mov_from_ar(_AREG_BSP, _NO_FENCE)))
#elif defined(__INTEL_COMPILER)
/* icc */
#include <asm/ia64regs.h>
#define ia64_get_bsp() ((char *) __getReg(_IA64_REG_AR_BSP))
#else
/* gcc */
static __inline__ char *
ia64_get_bsp(void)
{
	char	   *ret;

	/* the ;; is a "stop", seems to be required before fetching BSP */
	__asm__ __volatile__(
						 ";;\n"
						 "	mov	%0=ar.bsp	\n"
:						 "=r"(ret));

	return ret;
}
#endif
#endif							/* IA64 */


/*
 * set_stack_base: set up reference point for stack depth checking
 *
 * Returns the old reference point, if any.
 */
pg_stack_base_t
set_stack_base(void)
{
#ifndef HAVE__BUILTIN_FRAME_ADDRESS
	char		stack_base;
#endif
	pg_stack_base_t old;

#if defined(__ia64__) || defined(__ia64)
	old.stack_base_ptr = stack_base_ptr;
	old.register_stack_base_ptr = register_stack_base_ptr;
#else
	old = stack_base_ptr;
#endif

	/*
	 * Set up reference point for stack depth checking.  On recent gcc we use
	 * __builtin_frame_address() to avoid a warning about storing a local
	 * variable's address in a long-lived variable.
	 */
#ifdef HAVE__BUILTIN_FRAME_ADDRESS
	stack_base_ptr = __builtin_frame_address(0);
#else
	stack_base_ptr = &stack_base;
#endif
#if defined(__ia64__) || defined(__ia64)
	register_stack_base_ptr = ia64_get_bsp();
#endif

	return old;
}

/*
 * restore_stack_base: restore reference point for stack depth checking
 *
 * This can be used after set_stack_base() to restore the old value. This
 * is currently only used in PL/Java. When PL/Java calls a backend function
 * from different thread, the thread's stack is at a different location than
 * the main thread's stack, so it sets the base pointer before the call, and
 * restores it afterwards.
 */
void
restore_stack_base(pg_stack_base_t base)
{
#if defined(__ia64__) || defined(__ia64)
	stack_base_ptr = base.stack_base_ptr;
	register_stack_base_ptr = base.register_stack_base_ptr;
#else
	stack_base_ptr = base;
#endif
}

/*
 * check_stack_depth/stack_is_too_deep: check for excessively deep recursion
 *
 * This should be called someplace in any recursive routine that might possibly
 * recurse deep enough to overflow the stack.  Most Unixen treat stack
 * overflow as an unrecoverable SIGSEGV, so we want to error out ourselves
 * before hitting the hardware limit.
 *
 * check_stack_depth() just throws an error summarily.  stack_is_too_deep()
 * can be used by code that wants to handle the error condition itself.
 */
void
check_stack_depth(void)
{
	if (stack_is_too_deep())
	{
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 errmsg("stack depth limit exceeded"),
				 errhint("Increase the configuration parameter \"max_stack_depth\" (currently %dkB), "
						 "after ensuring the platform's stack depth limit is adequate.",
						 max_stack_depth)));
	}
}

bool
stack_is_too_deep(void)
{
	char		stack_top_loc;
	long		stack_depth;

	/*
	 * Compute distance from reference point to my local variables
	 */
	stack_depth = (long) (stack_base_ptr - &stack_top_loc);

	/*
	 * Take abs value, since stacks grow up on some machines, down on others
	 */
	if (stack_depth < 0)
		stack_depth = -stack_depth;

	/*
	 * Trouble?
	 *
	 * The test on stack_base_ptr prevents us from erroring out if called
	 * during process setup or in a non-backend process.  Logically it should
	 * be done first, but putting it here avoids wasting cycles during normal
	 * cases.
	 */
	if (stack_depth > max_stack_depth_bytes &&
		stack_base_ptr != NULL)
		return true;

	/*
	 * On IA64 there is a separate "register" stack that requires its own
	 * independent check.  For this, we have to measure the change in the
	 * "BSP" pointer from PostgresMain to here.  Logic is just as above,
	 * except that we know IA64's register stack grows up.
	 *
	 * Note we assume that the same max_stack_depth applies to both stacks.
	 */
#if defined(__ia64__) || defined(__ia64)
	stack_depth = (long) (ia64_get_bsp() - register_stack_base_ptr);

	if (stack_depth > max_stack_depth_bytes &&
		register_stack_base_ptr != NULL)
		return true;
#endif							/* IA64 */

	return false;
}

/* GUC check hook for max_stack_depth */
bool
check_max_stack_depth(int *newval, void **extra, GucSource source)
{
	long		newval_bytes = *newval * 1024L;
	long		stack_rlimit = get_stack_depth_rlimit();

	if (stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP)
	{
		GUC_check_errdetail("\"max_stack_depth\" must not exceed %ldkB.",
							(stack_rlimit - STACK_DEPTH_SLOP) / 1024L);
		GUC_check_errhint("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.");
		return false;
	}
	return true;
}

/* GUC assign hook for max_stack_depth */
void
assign_max_stack_depth(int newval, void *extra)
{
	long		newval_bytes = newval * 1024L;

	max_stack_depth_bytes = newval_bytes;
}


/*
 * set_debug_options --- apply "-d N" command line option
 *
 * -d is not quite the same as setting log_min_messages because it enables
 * other output options.
 */
void
set_debug_options(int debug_flag, GucContext context, GucSource source)
{
	if (debug_flag > 0)
	{
		char		debugstr[64];

		sprintf(debugstr, "debug%d", debug_flag);
		SetConfigOption("log_min_messages", debugstr, context, source);
	}
	else
		SetConfigOption("log_min_messages", "notice", context, source);

	if (debug_flag >= 1 && context == PGC_POSTMASTER)
	{
		SetConfigOption("log_connections", "true", context, source);
		SetConfigOption("log_disconnections", "true", context, source);
	}
	if (debug_flag >= 2)
		SetConfigOption("log_statement", "all", context, source);
	if (debug_flag >= 3)
		SetConfigOption("debug_print_parse", "true", context, source);
	if (debug_flag >= 4)
		SetConfigOption("debug_print_plan", "true", context, source);
	if (debug_flag >= 5)
		SetConfigOption("debug_print_rewritten", "true", context, source);
}


bool
set_plan_disabling_options(const char *arg, GucContext context, GucSource source)
{
	const char *tmp = NULL;

	switch (arg[0])
	{
		case 's':				/* seqscan */
			tmp = "enable_seqscan";
			break;
		case 'i':				/* indexscan */
			tmp = "enable_indexscan";
			break;
		case 'o':				/* indexonlyscan */
			tmp = "enable_indexonlyscan";
			break;
		case 'b':				/* bitmapscan */
			tmp = "enable_bitmapscan";
			break;
		case 't':				/* tidscan */
			tmp = "enable_tidscan";
			break;
		case 'n':				/* nestloop */
			tmp = "enable_nestloop";
			break;
		case 'm':				/* mergejoin */
			tmp = "enable_mergejoin";
			break;
		case 'h':				/* hashjoin */
			tmp = "enable_hashjoin";
			break;
	}
	if (tmp)
	{
		SetConfigOption(tmp, "false", context, source);
		return true;
	}
	else
		return false;
}


const char *
get_stats_option_name(const char *arg)
{
	switch (arg[0])
	{
		case 'p':
			if (optarg[1] == 'a')	/* "parser" */
				return "log_parser_stats";
			else if (optarg[1] == 'l')	/* "planner" */
				return "log_planner_stats";
			break;

		case 'e':				/* "executor" */
			return "log_executor_stats";
			break;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 * process_postgres_switches
 *	   Parse command line arguments for PostgresMain
 *
 * This is called twice, once for the "secure" options coming from the
 * postmaster or command line, and once for the "insecure" options coming
 * from the client's startup packet.  The latter have the same syntax but
 * may be restricted in what they can do.
 *
 * argv[0] is ignored in either case (it's assumed to be the program name).
 *
 * ctx is PGC_POSTMASTER for secure options, PGC_BACKEND for insecure options
 * coming from the client, or PGC_SU_BACKEND for insecure options coming from
 * a superuser client.
 *
 * If a database name is present in the command line arguments, it's
 * returned into *dbname (this is allowed only if *dbname is initially NULL).
 * ----------------------------------------------------------------
 */
void
process_postgres_switches(int argc, char *argv[], GucContext ctx,
						  const char **dbname)
{
	bool		secure = (ctx == PGC_POSTMASTER);
	int			errs = 0;
	GucSource	gucsource;
	int			flag;

	if (secure)
	{
		gucsource = PGC_S_ARGV; /* switches came from command line */

		/* Ignore the initial --single argument, if present */
		if (argc > 1 && strcmp(argv[1], "--single") == 0)
		{
			argv++;
			argc--;
		}
	}
	else
	{
		gucsource = PGC_S_CLIENT;	/* switches came from client */
	}

#ifdef HAVE_INT_OPTERR

	/*
	 * Turn this off because it's either printed to stderr and not the log
	 * where we'd want it, or argv[0] is now "--single", which would make for
	 * a weird error message.  We print our own error message below.
	 */
	opterr = 0;
#endif

	/*
	 * Parse command-line options.  CAUTION: keep this in sync with
	 * postmaster/postmaster.c (the option sets should not conflict) and with
	 * the common help() function in main/main.c.
	 */
	while ((flag = getopt(argc, argv, "B:bc:C:D:d:EeFf:h:ijk:lMm:N:nOPp:r:R:S:sTt:v:W:-:")) != -1)
	{
		switch (flag)
		{
			case 'B':
				SetConfigOption("shared_buffers", optarg, ctx, gucsource);
				break;

			case 'b':
				/* Undocumented flag used for binary upgrades */
				if (secure)
					IsBinaryUpgrade = true;
				break;

			case 'C':
				/* ignored for consistency with the postmaster */
				break;

			case 'D':
				if (secure)
					userDoption = strdup(optarg);
				break;

			case 'd':
				set_debug_options(atoi(optarg), ctx, gucsource);
				break;

			case 'E':
				if (secure)
					EchoQuery = true;
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", ctx, gucsource);
				break;

			case 'F':
				SetConfigOption("fsync", "false", ctx, gucsource);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, ctx, gucsource))
					errs++;
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, ctx, gucsource);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", ctx, gucsource);
				break;

			case 'j':
				if (secure)
					UseSemiNewlineNewline = true;
				break;

			case 'k':
				SetConfigOption("unix_socket_directories", optarg, ctx, gucsource);
				break;

			case 'l':
				SetConfigOption("ssl", "true", ctx, gucsource);
				break;

			case 'M':
				/* Undocumented flag used for mutating a directory that was a copy of a
				 * master data directory and needs to now be a segment directory. Only
				 * use on the first time the segment is started, and only use in
				 * utility mode, as changes will be destructive, and will assume that
				 * the segment has never participated in a distributed
				 * transaction.*/
				if (secure)
					ConvertMasterDataDirToSegment = true;
				break;

			case 'm':
				/*
				 * In maintenance mode:
				 * 	1. allow DML on catalog table
				 * 	2. allow DML on segments
				 */
				SetConfigOption("maintenance_mode",         "true", ctx, gucsource);
				SetConfigOption("allow_segment_DML",        "true", ctx, gucsource);
				SetConfigOption("allow_system_table_mods",  "true",  ctx, gucsource);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, ctx, gucsource);
				break;

			case 'n':
				/* ignored for consistency with postmaster */
				break;

			case 'O':
				/* Only use in single user mode */
				SetConfigOption("allow_system_table_mods", "true", ctx, gucsource);
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", ctx, gucsource);
				break;

			case 'p':
				SetConfigOption("port", optarg, ctx, gucsource);
				break;

			case 'r':
				/* send output (stdout and stderr) to the given file */
				if (secure)
					strlcpy(OutputFileName, optarg, MAXPGPATH);
				break;

			case 'R':
				terminal_fd = atoi(optarg);
				if (terminal_fd == -1)
				{
					/*
					 * Allow file descriptor closing to be bypassed via -1.
					 * We just duplicate sterr.  This is useful for
					 * single-user mode.
					 */
					terminal_fd = dup(2);
				}
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, ctx, gucsource);
				break;

			case 's':
				SetConfigOption("log_statement_stats", "true", ctx, gucsource);
				break;

			case 'T':
				/* ignored for consistency with the postmaster */
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
						SetConfigOption(tmp, "true", ctx, gucsource);
					else
						errs++;
					break;
				}

			case 'v':

				/*
				 * -v is no longer used in normal operation, since
				 * FrontendProtocol is already set before we get here. We keep
				 * the switch only for possible use in standalone operation,
				 * in case we ever support using normal FE/BE protocol with a
				 * standalone backend.
				 */
				if (secure)
					FrontendProtocol = (ProtocolVersion) atoi(optarg);
				break;

			case 'W':
				SetConfigOption("post_auth_delay", optarg, ctx, gucsource);
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (flag == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}
					SetConfigOption(name, value, ctx, gucsource);
					free(name);
					if (value)
						free(value);
					break;
				}

			default:
				errs++;
				break;
		}

		if (errs)
			break;
	}

	/*
	 * Optional database name should be there only if *dbname is NULL.
	 */
	if (!errs && dbname && *dbname == NULL && argc - optind >= 1)
		*dbname = strdup(argv[optind++]);

	if (errs || argc != optind)
	{
		if (errs)
			optind--;			/* complain about the previous argument */

		/* spell the error message a bit differently depending on context */
		if (IsUnderPostmaster)
			ereport(FATAL,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid command-line argument for server process: %s", argv[optind]),
					errhint("Try \"%s --help\" for more information.", progname));
		else
			ereport(FATAL,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("%s: invalid command-line argument: %s",
						   progname, argv[optind]),
					errhint("Try \"%s --help\" for more information.", progname));
	}

	/*
	 * Reset getopt(3) library so that it will work correctly in subprocesses
	 * or when this function is called a second time with another array.
	 */
	optind = 1;
#ifdef HAVE_INT_OPTRESET
	optreset = 1;				/* some systems need this too */
#endif
}

/*
 * Throw an error if we're a GPDB specific message handler process.
 *
 * This is used to forbid anything else than simple query protocol messages in
 * a GPDB specific message handler process (e.g. FTS or fault message
 * handlers).  'firstchar' specifies what kind of a forbidden message was
 * received, and is used to construct the error message.
 */
static void
check_forbidden_in_gpdb_handlers(int firstchar)
{
	if (am_ftshandler || am_faulthandler)
	{
		switch (firstchar)
		{
			case 'Q':
			case 'X':
			case (char) EOF:
				return;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("protocol '%c' is not supported in a GPDB message handler connection",
								firstchar)));
		}
	}
}

static void
forbidden_in_retrieve_handler(int firstchar)
{
	if (am_cursor_retrieve_handler)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("protocol '%c' is not supported in a GPDB parallel retrieve cursor connection",
						firstchar)));
}

/* ----------------------------------------------------------------
 * PostgresMain
 *	   postgres main loop -- all backends, interactive or otherwise start here
 *
 * argc/argv are the command line arguments to be used.  (When being forked
 * by the postmaster, these are not the original argv array of the process.)
 * dbname is the name of the database to connect to, or NULL if the database
 * name should be extracted from the command line arguments or defaulted.
 * username is the PostgreSQL user name to be used for the session.
 * ----------------------------------------------------------------
 */
void
PostgresMain(int argc, char *argv[],
			 const char *dbname,
			 const char *username)
{
	int			firstchar;
	StringInfoData input_message;
	sigjmp_buf	local_sigjmp_buf;
	volatile bool send_ready_for_query = true;
	bool		idle_in_transaction_timeout_enabled = false;
	bool		idle_session_timeout_enabled = false;

	/*
	 * CDB: Catch program error signals.
	 *
	 * Save our main thread-id for comparison during signals.
	 */
	main_tid = pthread_self();

	/* Initialize startup process environment if necessary. */
	if (!IsUnderPostmaster)
		InitStandaloneProcess(argv[0]);

#ifndef WIN32
	PostmasterPriority = getpriority(PRIO_PROCESS, 0);
#endif

	set_ps_display("startup");

	SetProcessingMode(InitProcessing);

	/*
	 * Set default values for command-line options.
	 */
	EchoQuery = false;

	if (!IsUnderPostmaster)
		InitializeGUCOptions();

	/*
	 * Parse command-line options.
	 */
	process_postgres_switches(argc, argv, PGC_POSTMASTER, &dbname);

	/* Must have gotten a database name, or have a default (the username) */
	if (dbname == NULL)
	{
		dbname = username;
		if (dbname == NULL)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: no database nor user name specified",
							progname)));
	}

	/* Acquire configuration parameters, unless inherited from postmaster */
	if (!IsUnderPostmaster)
	{
		if (!SelectConfigFiles(userDoption, progname))
			proc_exit(1);

        /*
	     * Remember stand-alone backend startup time.
         * CDB: Moved this up from below for use in error message headers.
         */
	    PgStartTime = GetCurrentTimestamp();
	}

	/*
	 * Set up signal handlers.  (InitPostmasterChild or InitStandaloneProcess
	 * has already set up BlockSig and made that the active signal mask.)
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal before
	 * we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in the
	 * postmaster.  If such a signal arrives before we are able to change the
	 * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
	 * handler in the postmaster to reserve the signal. (Of course, this isn't
	 * an issue for signals that are locally generated, such as SIGALRM and
	 * SIGPIPE.)
	 */
	if (am_walsender)
		WalSndSignals();
	else
	{
		pqsignal(SIGHUP, SignalHandlerForConfigReload);
		pqsignal(SIGINT, StatementCancelHandler);	/* cancel current query */
		pqsignal(SIGTERM, die); /* cancel current query and exit */

		/*
		 * In a postmaster child backend, replace SignalHandlerForCrashExit
		 * with quickdie, so we can tell the client we're dying.
		 *
		 * In a standalone backend, SIGQUIT can be generated from the keyboard
		 * easily, while SIGTERM cannot, so we make both signals do die()
		 * rather than quickdie().
		 */
		if (IsUnderPostmaster)
			pqsignal(SIGQUIT, quickdie);	/* hard crash time */
		else
			pqsignal(SIGQUIT, die); /* cancel current query and exit */
		InitializeTimeouts();	/* establishes SIGALRM handler */

		/*
		 * Ignore failure to write to frontend. Note: if frontend closes
		 * connection, we will notice it and exit cleanly when control next
		 * returns to outer loop.  This seems safer than forcing exit in the
		 * midst of output during who-knows-what operation...
		 */
		pqsignal(SIGPIPE, SIG_IGN);
		pqsignal(SIGUSR1, procsignal_sigusr1_handler);
		pqsignal(SIGUSR2, SIG_IGN);
		pqsignal(SIGFPE, FloatExceptionHandler);

		/*
		 * Reset some signals that are accepted by postmaster but not by
		 * backend
		 */
		pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
									 * platforms */
#ifndef _WIN32
#ifdef SIGILL
		pqsignal(SIGILL, CdbProgramErrorHandler);
#endif
#ifdef SIGSEGV
		pqsignal(SIGSEGV, CdbProgramErrorHandler);
#endif
#ifdef SIGBUS
		pqsignal(SIGBUS, CdbProgramErrorHandler);
#endif
#endif
	}

	if (!IsUnderPostmaster)
	{
		/*
		 * Validate we have been given a reasonable-looking DataDir (if under
		 * postmaster, assume postmaster did this already).
		 */
		checkDataDir();

		/* Change into DataDir (if under postmaster, was done already) */
		ChangeToDataDir();

		/*
		 * Create lockfile for data directory.
		 */
		CreateDataDirLockFile(false);

		/* read control file (error checking and contains config ) */
		LocalProcessControlFile(false);

		/*
		 * process any libraries that should be preloaded at postmaster start
		 */
		process_shared_preload_libraries();

		/* Initialize MaxBackends (if under postmaster, was done already) */
		InitializeMaxBackends();

		/*
		 * Now that modules have been loaded, we can process any custom resource
		 * managers specified in the wal_consistency_checking GUC.
		 */
		InitializeWalConsistencyChecking();
	}

	/* Early initialization */
	BaseInit();

	if (!IsUnderPostmaster)
	{
		/*
		* Initialize kmgr for cluster encryption. Since kmgr needs to attach to
		* shared memory the initialization must be called after BaseInit().
		* we need some information from the controlFile, 
		* so must call the InitializeKmgr after LocalProcessControlFile.
		*/
		InitializeKmgr();
		InitializeBufferEncryption();

		if (terminal_fd != -1)
			close(terminal_fd);
	}

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifdef EXEC_BACKEND
	if (!IsUnderPostmaster)
		InitProcess();
#else
	InitProcess();
#endif

	/* We need to allow SIGINT, etc during the initial transaction */
	PG_SETMASK(&UnBlockSig);

	/*
	 * General initialization.
	 *
	 * NOTE: if you are tempted to add code in this vicinity, consider putting
	 * it inside InitPostgres() instead.  In particular, anything that
	 * involves database access should be there, not here.
	 */
	InitPostgres(dbname, InvalidOid, username, InvalidOid, NULL, false);

	/*
	 * If the PostmasterContext is still around, recycle the space; we don't
	 * need it anymore after InitPostgres completes.  Note this does not trash
	 * *MyProcPort, because ConnCreate() allocated that space with malloc()
	 * ... else we'd need to copy the Port data first.  Also, subsidiary data
	 * such as the username isn't lost either; see ProcessStartupPacket().
	 */
	if (PostmasterContext)
	{
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;
	}

	SetProcessingMode(NormalProcessing);

	/*
	 * Now all GUC states are fully set up.  Report them to client if
	 * appropriate.
	 */
	BeginReportingGUCOptions();

	/*
	 * Also set up handler to log session end; we have to wait till now to be
	 * sure Log_disconnections has its final value.
	 */
	if (IsUnderPostmaster && Log_disconnections)
		on_proc_exit(log_disconnections, 0);

	pgstat_report_connect(MyDatabaseId);

	/* Perform initialization specific to a WAL sender process. */
	if (am_walsender)
		InitWalSender();

	/*
	 * process any libraries that should be preloaded at backend start (this
	 * likewise can't be done until GUC settings are complete)
	 */
	process_session_preload_libraries();

	/*
	 * DA requires these be cleared at start
	 */
	DtxContextInfo_Reset(&QEDtxContextInfo);

	/*
	 * Send this backend's cancellation info to the frontend.
	 */
	if (whereToSendOutput == DestRemote)
	{
		StringInfoData buf;

		pq_beginmessage(&buf, 'K');
		pq_sendint32(&buf, (int32) MyProcPid);
		pq_sendint32(&buf, (int32) MyCancelKey);
		pq_endmessage(&buf);
		/* Need not flush since ReadyForQuery will do it. */
	}

	/* Also send GPDB QE-backend startup info (motion listener, version). */
	if (!(am_ftshandler || am_faulthandler) && Gp_role == GP_ROLE_EXECUTE)
	{
#ifdef FAULT_INJECTOR
		if (SIMPLE_FAULT_INJECTOR("send_qe_details_init_backend") != FaultInjectorTypeSkip)
#endif
			sendQEDetails();
	}

	/* Welcome banner for standalone case */
	if (whereToSendOutput == DestDebug)
		printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION);

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	/*
	 * Create memory context and buffer used for RowDescription messages. As
	 * SendRowDescriptionMessage(), via exec_describe_statement_message(), is
	 * frequently executed for ever single statement, we don't want to
	 * allocate a separate buffer every time.
	 */
	row_description_context = AllocSetContextCreate(TopMemoryContext,
													"RowDescriptionContext",
													ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(row_description_context);
	initStringInfo(&row_description_buf);
	MemoryContextSwitchTo(TopMemoryContext);


	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort the
	 * current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that this function's signal mask
	 * (to wit, UnBlockSig) will be restored when longjmp'ing to here.  This
	 * is essential in case we longjmp'd out of a signal handler on a platform
	 * where that leaves the signal blocked.  It's not redundant with the
	 * unblock in AbortTransaction() because the latter is only called if we
	 * were inside a transaction.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.  The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for outer-level
		 * error recovery, such as adjusting the FE/BE protocol status.
		 */

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Forget any pending QueryCancel request, since we're returning to
		 * the idle loop anyway, and cancel any active timeout requests.  (In
		 * future we might want to allow some timeout requests to survive, but
		 * at minimum it'd be necessary to do reschedule_timeouts(), in case
		 * we got here because of a query cancel interrupting the SIGALRM
		 * interrupt handler.)	Note in particular that we must clear the
		 * statement and lock timeout indicators, to prevent any future plain
		 * query cancels from being misreported as timeouts in case we're
		 * forgetting a timeout cancel.
		 */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */
		QueryFinishPending = false;

		/* Not reading from the client anymore. */
		DoingCommandRead = false;
		DisableClientWaitTimeoutInterrupt();

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * Make sure debug_query_string gets reset before we possibly clobber
		 * the storage it points at.
		 */
		if (debug_query_string != NULL)
		{
			elog_exception_statement(debug_query_string);
			debug_query_string = NULL;
		}

		/*
		 * Abort the current transaction in order to recover.
		 */
		AbortCurrentTransaction();

		if (am_walsender)
			WalSndErrorCleanup();

		PortalErrorCleanup();

		/*
		 * We can't release replication slots inside AbortTransaction() as we
		 * need to be able to start and abort transactions while having a slot
		 * acquired. But we never need to hold them across top level errors,
		 * so releasing here is fine. There's another cleanup in ProcKill()
		 * ensuring we'll correctly cleanup on FATAL errors as well.
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		/* We also want to cleanup temporary slots on error. */
		ReplicationSlotCleanup();

		jit_reset_after_error();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		/*
		 * If we were handling an extended-query-protocol message, initiate
		 * skip till next Sync.  This also causes us not to issue
		 * ReadyForQuery (until we get Sync).
		 */
		if (doing_extended_query_message)
			ignore_till_sync = true;

		/* We don't have a transaction command open anymore */
		xact_started = false;

		/* Inform Vmem tracker that the current process has finished cleanup */
		RunawayCleaner_RunawayCleanupDoneForProcess(false /* ignoredCleanup */);

		/*
		 * If an error occurred while we were reading a message from the
		 * client, we have potentially lost track of where the previous
		 * message ends and the next one begins.  Even though we have
		 * otherwise recovered from the error, we cannot safely read any more
		 * messages from the client, so there isn't much we can do with the
		 * connection anymore.
		 */
		if (pq_is_reading_msg())
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("terminating connection because protocol synchronization was lost")));

		GpRecoveryFromError();
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	if (!ignore_till_sync)
		send_ready_for_query = true;	/* initially, or after error */

	/*
	 * Non-error queries loop here.
	 */

	for (;;)
	{
		/*
		 * At top of loop, reset extended-query-message flag, so that any
		 * errors encountered in "idle" state don't provoke skip.
		 */
		doing_extended_query_message = false;

		/*
		 * Release storage left over from prior query cycle, and create a new
		 * query input buffer in the cleared MessageContext.
		 */
		MemoryContextSwitchTo(MessageContext);
		MemoryContextResetAndDeleteChildren(MessageContext);
		VmemTracker_ResetMaxVmemReserved();
		VmemTracker_ResetWaiver();

		initStringInfo(&input_message);

		/* Reset elog globals */
		currentSliceId = UNSET_SLICE_ID;
		if (Gp_role == GP_ROLE_EXECUTE)
			gp_command_count = 0;

		/*
		 * Do deactiving and runaway detecting before ReadyForQuery(),
		 * so any OOM errors of current query will not muddle following
		 * queries
		 */
		IdleTracker_DeactivateProcess();

		/*
		 * Also consider releasing our catalog snapshot if any, so that it's
		 * not preventing advance of global xmin while we wait for the client.
		 */
		InvalidateCatalogSnapshotConditionally();

		/*
		 * (1) If we've reached idle state, tell the frontend we're ready for
		 * a new query.
		 *
		 * Note: this includes fflush()'ing the last of the prior output.
		 *
		 * This is also a good time to send collected statistics to the
		 * collector, and to update the PS stats display.  We avoid doing
		 * those every time through the message loop because it'd slow down
		 * processing of batched messages, and because we don't want to report
		 * uncommitted updates (that confuses autovacuum).  The notification
		 * processor wants a call too, if we are not in a transaction block.
		 *
		 * Also, if an idle timeout is enabled, start the timer for that.
		 */
		if (send_ready_for_query)
		{
			char activity[50];
			memset(activity, 0, sizeof(activity));
			int remain = sizeof(activity);

			if (am_cursor_retrieve_handler)
			{
				strncpy(activity, "[retrieve] ", sizeof(activity));
				remain -= strlen(activity);
			}
			if (IsAbortedTransactionBlockState())
			{
				strncat(activity, "idle in transaction (aborted)", remain);
				set_ps_display(activity);
				pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL);

				/* Start the idle-in-transaction timer */
				if (IdleInTransactionSessionTimeout > 0 && Gp_role != GP_ROLE_EXECUTE)
				{
					idle_in_transaction_timeout_enabled = true;
					enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
										 IdleInTransactionSessionTimeout);
				}
			}
			else if (IsTransactionOrTransactionBlock())
			{
				strncat(activity, "idle in transaction", remain);
				set_ps_display(activity);
				pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);

				/* Start the idle-in-transaction timer */
				if (IdleInTransactionSessionTimeout > 0 && Gp_role != GP_ROLE_EXECUTE)
				{
					idle_in_transaction_timeout_enabled = true;
					enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
										 IdleInTransactionSessionTimeout);
				}
			}
			else
			{
				/*
				 * Process incoming notifies (including self-notifies), if
				 * any, and send relevant messages to the client.  Doing it
				 * here helps ensure stable behavior in tests: if any notifies
				 * were received during the just-finished transaction, they'll
				 * be seen by the client before ReadyForQuery is.
				 */
				if (notifyInterruptPending)
					ProcessNotifyInterrupt(false);

				pgstat_report_stat(false);
				pgstat_report_queuestat();

				strncat(activity, "idle", remain);
				set_ps_display(activity);
				pgstat_report_activity(STATE_IDLE, NULL);

				/* Start the idle-session timer */
				if (IdleSessionTimeout > 0)
				{
					idle_session_timeout_enabled = true;
					enable_timeout_after(IDLE_SESSION_TIMEOUT,
										 IdleSessionTimeout);
				}
			}

			/* Report any recently-changed GUC options */
			ReportChangedGUCOptions();

			ReadyForQuery(whereToSendOutput);
			send_ready_for_query = false;
		}

		/*
		 * (2) Allow asynchronous signals to be executed immediately if they
		 * come in while we are waiting for client input. (This must be
		 * conditional since we don't want, say, reads on behalf of COPY FROM
		 * STDIN doing the same thing.)
		 */
		DoingCommandRead = true;

		/*
		 * (2b) Check for temp table delete reset session work.
		 * Also clean up idle resources.
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			GpDropTempTables();
			StartIdleResourceCleanupTimers();
		}

		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadCommand(&input_message);

		if (Gp_role == GP_ROLE_DISPATCH)
			CancelIdleResourceCleanupTimers();

		/*
		 * Reset QueryFinishPending flag, so that if we received a delayed
		 * query finish requested after we had already finished processing
		 * the previous command, we don't prematurely finish the next
		 * command.
		 */
		QueryFinishPending = false;

		IdleTracker_ActivateProcess();

		/*
		 * (4) turn off the idle-in-transaction and idle-session timeouts, if
		 * active.  We do this before step (5) so that any last-moment timeout
		 * is certain to be detected in step (5).
		 *
		 * At most one of these timeouts will be active, so there's no need to
		 * worry about combining the timeout.c calls into one.
		 */
		if (idle_in_transaction_timeout_enabled)
		{
			disable_timeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
			idle_in_transaction_timeout_enabled = false;
		}
		if (idle_session_timeout_enabled)
		{
			disable_timeout(IDLE_SESSION_TIMEOUT, false);
			idle_session_timeout_enabled = false;
		}

		/*
		 * (5) disable async signal conditions again.
		 *
		 * Query cancel is supposed to be a no-op when there is no query in
		 * progress, so if a query cancel arrived while we were idle, just
		 * reset QueryCancelPending. ProcessInterrupts() has that effect when
		 * it's called when DoingCommandRead is set, so check for interrupts
		 * before resetting DoingCommandRead.
		 */
		CHECK_FOR_INTERRUPTS();
		DoingCommandRead = false;

		/*
		 * (6) check for any other interesting events that happened while we
		 * slept.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * (7) process the command.  But ignore it if we're skipping till
		 * Sync.
		 */
		if (ignore_till_sync && firstchar != EOF)
			continue;

		/* last txn abort, try to synchronize guc to cached QE */
		if(Gp_role == GP_ROLE_DISPATCH && gp_guc_restore_list)
			restore_guc_to_QE();


		// firstchar may be -1, it will cause AssertFailed if we enable cassert
		ereport((Debug_print_full_dtm ? LOG : DEBUG5),
				(errmsg_internal("First char: '%d'; gp_role = '%s'.", firstchar, role_to_string(Gp_role))));

		check_forbidden_in_gpdb_handlers(firstchar);

		switch (firstchar)
		{
			case 'Q':			/* simple query */
				{
					const char *query_string;

                    elog(DEBUG1, "Message type %c received by from libpq, len = %d", firstchar, input_message.len); /* TODO: Remove this */

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();
                    query_string = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Simple query stmt: %s.",query_string);

					if (am_walsender)
					{
						if (!exec_replication_command(query_string))
						{
							if (exec_simple_query_hook)
								exec_simple_query_hook(query_string);
							else
								exec_simple_query(query_string);
						}
					}
					else if (am_ftshandler)
						HandleFtsMessage(query_string);
					else if (am_faulthandler)
						HandleFaultMessage(query_string);
					else if (exec_simple_query_hook)
						exec_simple_query_hook(query_string);
					else
						exec_simple_query(query_string);

					send_ready_for_query = true;
				}
				break;
            case 'M': /* MPP dispatched stmt from QD */
				{
					/*
					 * This is exactly like 'Q' above except we peel off and
					 * set the snapshot information right away.
					 */
					const char *query_string = "";

					const char *serializedDtxContextInfo = NULL;
					const char *serializedPlantree = NULL;
					const char *serializedQueryDispatchDesc = NULL;
					const char *resgroupInfoBuf = NULL;

					int is_hs_dispatch;
					int query_string_len = 0;
					int serializedDtxContextInfolen = 0;
					int serializedPlantreelen = 0;
					int serializedQueryDispatchDesclen = 0;
					int resgroupInfoLen = 0;
					TimestampTz statementStart;
					Oid suid;
					Oid ouid;
					Oid cuid;

					if (Gp_role != GP_ROLE_EXECUTE)
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("MPP protocol messages are only supported in QD - QE connections")));
					/*
					 * QD performs the function body check, hence QE doesn't
					 * need to do the check again. Turn off the check in QE
					 * process as an optimization. Also, helps eliminate the
					 * need for having this GUC in-sync between QD and QE.
					 */
					check_function_bodies=false;

					/* Set statement_timestamp() */
 					SetCurrentStatementStartTimestamp();

					/* get the client command serial# */
					gp_command_count = pq_getmsgint(&input_message, 4);

					elog(DEBUG1, "Message type %c received by from libpq, len = %d", firstchar, input_message.len); /* TODO: Remove this */

					/* Get the userid info  (session, outer, current) */
					suid = pq_getmsgint(&input_message, 4);
					ouid = pq_getmsgint(&input_message, 4);
					cuid = pq_getmsgint(&input_message, 4);

					statementStart = pq_getmsgint64(&input_message);

					/* check if the message is from standby QD and is expected */
					is_hs_dispatch = pq_getmsgint(&input_message, 4);
					if (is_hs_dispatch == 0 && IS_HOT_STANDBY_QE())
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("mirror segments can only process MPP protocol messages from standby QD"),
								 errhint("Exit the current session and re-connect.")));
					else if (is_hs_dispatch != 0 && !IS_HOT_STANDBY_QE())
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("primary segments can only process MPP protocol messages from primary QD"),
								 errhint("Exit the current session and re-connect.")));

					query_string_len = pq_getmsgint(&input_message, 4);
					serializedPlantreelen = pq_getmsgint(&input_message, 4);
					serializedQueryDispatchDesclen = pq_getmsgint(&input_message, 4);
					serializedDtxContextInfolen = pq_getmsgint(&input_message, 4);

					/* read in the DTX context info */
					if (serializedDtxContextInfolen == 0)
						serializedDtxContextInfo = NULL;
					else
						serializedDtxContextInfo = pq_getmsgbytes(&input_message,serializedDtxContextInfolen);

					DtxContextInfo_Deserialize(serializedDtxContextInfo, serializedDtxContextInfolen, &TempDtxContextInfo);
					if (TempDtxContextInfo.distributedXid != InvalidDistributedTransactionId &&
						!IS_QUERY_DISPATCHER()) /* On segments only */
					{
						/*
						 * In theory we do not need to track nextGxid on
						 * segments since we generate gxid on the coordinator,
						 * but we still do this due to:
						 * 1. For possible debuggging purpose.
						 * 2. pg_resetwal on the coordinator needs this value
						 *    on all the segments for nextGxid guessing.
						 *    Normally we do not need to use pg_resetwal since
						 *    there is coordinator failover but pg_resetwal
						 *    is still possibly useful in some scenarios.
						 * 3. The related code change on segments are lightweight.
						 */
						SpinLockAcquire(shmGxidGenLock);
						if (TempDtxContextInfo.distributedXid > ShmemVariableCache->nextGxid)
							ShmemVariableCache->nextGxid = TempDtxContextInfo.distributedXid;
						SpinLockRelease(shmGxidGenLock);
					}

					/* get the query string and kick off processing. */
					if (query_string_len > 0)
						query_string = pq_getmsgbytes(&input_message,query_string_len);

					if (serializedPlantreelen > 0)
						serializedPlantree = pq_getmsgbytes(&input_message,serializedPlantreelen);

					if (serializedQueryDispatchDesclen > 0)
						serializedQueryDispatchDesc = pq_getmsgbytes(&input_message,serializedQueryDispatchDesclen);

					/*
					 * Always use the same GpIdentity.numsegments with QD on QEs
					 */
					numsegmentsFromQD = pq_getmsgint(&input_message, 4);

					resgroupInfoLen = pq_getmsgint(&input_message, 4);
					if (resgroupInfoLen > 0)
						resgroupInfoBuf = pq_getmsgbytes(&input_message, resgroupInfoLen);

					/* process local variables for temporary namespace */
					{
						Oid			tempNamespaceId, tempToastNamespaceId;

						tempNamespaceId = pq_getmsgint(&input_message, sizeof(tempNamespaceId));
						tempToastNamespaceId = pq_getmsgint(&input_message, sizeof(tempToastNamespaceId));
						SetTempNamespaceStateAfterBoot(tempNamespaceId, tempToastNamespaceId);
					}

					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "MPP dispatched stmt from QD: %s.",query_string);

					if (IsResGroupActivated() && resgroupInfoLen > 0)
						SwitchResGroupOnSegment(resgroupInfoBuf, resgroupInfoLen);

					/*
					 * GUC "is_supersuer" only provide value for SHOW to display,
					 * so it's useless on segments. SessionUserIsSuperuser is
					 * also designed to determine the value of is_superuser, so
					 * setting it to false on segments is fine.
					 */
					if (suid > 0)
						SetSessionUserId(suid, false); /* Set the session UserId */

					if (ouid > 0 && ouid != GetSessionUserId())
						SetCurrentRoleId(ouid, false); /* Set the outer UserId */

					setupQEDtxContext(&TempDtxContextInfo);

					if (cuid > 0)
						SetUserIdAndContext(cuid, false); /* Set current userid */

					if (serializedPlantreelen==0)
					{
						if (strncmp(query_string, "BEGIN", 5) == 0)
						{
							CommandDest dest = whereToSendOutput;
							QueryCompletion	qc;

							SetQueryCompletion(&qc, CMDTAG_BEGIN, 0);
							/*
							 * Special explicit BEGIN for COPY, etc.
							 * We've already begun it as part of setting up the context.
							 */
							elog((Debug_print_full_dtm ? LOG : DEBUG5), "PostgresMain explicit %s", query_string);

							// UNDONE: HACK
							pgstat_report_activity(STATE_RUNNING, "BEGIN");

							set_ps_display("BEGIN");

							BeginCommand(qc.commandTag, dest);

							EndCommand(&qc, dest, false);

						}
						else
						{
							if (exec_simple_query_hook)
								exec_simple_query_hook(query_string);
							else
								exec_simple_query(query_string);
						}
					}
					else
						exec_mpp_query(query_string,
									   serializedPlantree, serializedPlantreelen,
									   serializedQueryDispatchDesc, serializedQueryDispatchDesclen);

					SetUserIdAndSecContext(GetOuterUserId(), 0);

					SIMPLE_FAULT_INJECTOR("qe_exec_finished");
					send_ready_for_query = true;
				}
				break;

            case 'T': /* MPP dispatched dtx protocol command from QD */
				{
					DtxProtocolCommand dtxProtocolCommand;
					int loggingStrLen;
					const char *loggingStr;
					int gidLen;
					const char *gid;
					int serializedDtxContextInfolen;
					const char *serializedDtxContextInfo;

					if (Gp_role != GP_ROLE_EXECUTE)
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("MPP protocol messages are only supported in QD - QE connections")));

					elog(DEBUG1, "Message type %c received by from libpq, len = %d", firstchar, input_message.len); /* TODO: Remove this */

					/* get the transaction protocol command # */
					dtxProtocolCommand = (DtxProtocolCommand) pq_getmsgint(&input_message, 4);

					/* get the logging string length */
					loggingStrLen = pq_getmsgint(&input_message, 4);

					/* get the logging string */
					loggingStr = pq_getmsgbytes(&input_message,loggingStrLen);

					/* get the logging string length */
					gidLen = pq_getmsgint(&input_message, 4);

					/* get the logging string */
					gid = pq_getmsgbytes(&input_message,gidLen);

					serializedDtxContextInfolen = pq_getmsgint(&input_message, 4);

					/* read in the DTX context info */
					if (serializedDtxContextInfolen == 0)
						serializedDtxContextInfo = NULL;
					else
						serializedDtxContextInfo = pq_getmsgbytes(&input_message,serializedDtxContextInfolen);

					DtxContextInfo_Deserialize(serializedDtxContextInfo, serializedDtxContextInfolen, &TempDtxContextInfo);

					pq_getmsgend(&input_message);

					exec_mpp_dtx_protocol_command(dtxProtocolCommand, loggingStr, gid, &TempDtxContextInfo);

					send_ready_for_query = true;
            	}
				break;

			case 'P':			/* parse */
				{
					const char *stmt_name;
					const char *query_string;
					int			numParams;
					Oid		   *paramTypes = NULL;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					stmt_name = pq_getmsgstring(&input_message);
					query_string = pq_getmsgstring(&input_message);
					numParams = pq_getmsgint(&input_message, 2);
					if (numParams > 0)
					{
						paramTypes = (Oid *) palloc(numParams * sizeof(Oid));
						for (int i = 0; i < numParams; i++)
							paramTypes[i] = pq_getmsgint(&input_message, 4);
					}
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Parse: %s.",query_string);

					exec_parse_message(query_string, stmt_name,
									   paramTypes, numParams);
				}
				break;

			case 'B':			/* bind */
				forbidden_in_wal_sender(firstchar);

				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				/*
				 * this message is complex enough that it seems best to put
				 * the field extraction out-of-line
				 */
				exec_bind_message(&input_message);
				break;

			case 'E':			/* execute */
				{
					const char *portal_name;
					int64		max_rows;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					portal_name = pq_getmsgstring(&input_message);

					 /*Get the max rows but cast to int64 internally. */
					max_rows = (int64)pq_getmsgint(&input_message, 4);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Execute: %s.",portal_name);

					exec_execute_message(portal_name, max_rows);
				}
				break;

			case 'F':			/* fastpath function call */
				forbidden_in_wal_sender(firstchar);
				forbidden_in_retrieve_handler(firstchar);

				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				/* Report query to various monitoring facilities. */
				pgstat_report_activity(STATE_FASTPATH, NULL);
				set_ps_display("<FASTPATH>");

				elog((Debug_print_full_dtm ? LOG : DEBUG5), "Fast path function call.");

				/* start an xact for this function invocation */
				start_xact_command();

				/*
				 * Note: we may at this point be inside an aborted
				 * transaction.  We can't throw error for that until we've
				 * finished reading the function-call message, so
				 * HandleFunctionRequest() must check for it after doing so.
				 * Be careful not to do anything that assumes we're inside a
				 * valid transaction here.
				 */

				/* switch back to message context */
				MemoryContextSwitchTo(MessageContext);

				HandleFunctionRequest(&input_message);

				/* commit the function-invocation transaction */
				finish_xact_command();

				send_ready_for_query = true;
				break;

			case 'C':			/* close */
				{
					int			close_type;
					const char *close_target;

					forbidden_in_wal_sender(firstchar);

					close_type = pq_getmsgbyte(&input_message);
					close_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					switch (close_type)
					{
						case 'S':
							if (close_target[0] != '\0')
								DropPreparedStatement(close_target, false);
							else
							{
								/* special-case the unnamed statement */
								drop_unnamed_stmt();
							}
							break;
						case 'P':
							{
								Portal		portal;

								portal = GetPortalByName(close_target);
								if (PortalIsValid(portal))
									PortalDrop(portal, false);
							}
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid CLOSE message subtype %d",
											close_type)));
							break;
					}

					if (whereToSendOutput == DestRemote)
						pq_putemptymessage('3');	/* CloseComplete */
				}
				break;

			case 'D':			/* describe */
				{
					int			describe_type;
					const char *describe_target;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() (needed for xact) */
					SetCurrentStatementStartTimestamp();

					describe_type = pq_getmsgbyte(&input_message);
					describe_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Describe: %s.", describe_target);

					switch (describe_type)
					{
						case 'S':
							exec_describe_statement_message(describe_target);
							break;
						case 'P':
							exec_describe_portal_message(describe_target);
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid DESCRIBE message subtype %d",
											describe_type)));
							break;
					}
				}
				break;

			case 'H':			/* flush */
				pq_getmsgend(&input_message);
				if (whereToSendOutput == DestRemote)
					pq_flush();
				break;

			case 'S':			/* sync */
				pq_getmsgend(&input_message);
				finish_xact_command();
				send_ready_for_query = true;
				break;

				/*
				 * 'X' means that the frontend is closing down the socket. EOF
				 * means unexpected loss of frontend connection. Either way,
				 * perform normal shutdown.
				 */
			case EOF:

				/* for the statistics collector */
				pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;

				/* FALLTHROUGH */

			case 'X':

				/*
				 * Reset whereToSendOutput to prevent ereport from attempting
				 * to send any more messages to client.
				 */
				if (whereToSendOutput == DestRemote)
					whereToSendOutput = DestNone;

				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);
				break;

			case 'd':			/* copy data */
			case 'c':			/* copy done */
			case 'f':			/* copy fail */

				/*
				 * Accept but ignore these messages, per protocol spec; we
				 * probably got here because a COPY failed, and the frontend
				 * is still sending data.
				 */
				break;
			case '?':			/* Cloudberry sequence response */
				/*
				 * Accept but ignore this message, when QE process nextval
				 * it sends NOTIFY to QD and asks QD to send nextval back to
				 * QE, we probably got here because getting nextval on QD is
				 * failed, QD send '?' message back to QE and cancel all
				 * unfinished QEs, if the QE receives cancel before '?' message,
				 * the message will stay in the socket, next time when we ReadCommand
				 * we should ignore it.
				 */
				break;

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d",
								firstchar)));
		}
	}							/* end of input-reading loop */
}

/*
 * Throw an error if we're a WAL sender process.
 *
 * This is used to forbid anything else than simple query protocol messages
 * in a WAL sender process.  'firstchar' specifies what kind of a forbidden
 * message was received, and is used to construct the error message.
 */
static void
forbidden_in_wal_sender(int firstchar)
{
	if (am_walsender)
	{
		if (firstchar == 'F')
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("fastpath function calls not supported in a replication connection")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("extended query protocol not supported in a replication connection")));
	}
}


/*
 * Obtain platform stack depth limit (in bytes)
 *
 * Return -1 if unknown
 */
long
get_stack_depth_rlimit(void)
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_STACK)
	static long val = 0;

	/* This won't change after process launch, so check just once */
	if (val == 0)
	{
		struct rlimit rlim;

		if (getrlimit(RLIMIT_STACK, &rlim) < 0)
			val = -1;
		else if (rlim.rlim_cur == RLIM_INFINITY)
			val = LONG_MAX;
		/* rlim_cur is probably of an unsigned type, so check for overflow */
		else if (rlim.rlim_cur >= LONG_MAX)
			val = LONG_MAX;
		else
			val = rlim.rlim_cur;
	}
	return val;
#else							/* no getrlimit */
#if defined(WIN32) || defined(__CYGWIN__)
	/* On Windows we set the backend stack size in src/backend/Makefile */
	return WIN32_STACK_RLIMIT;
#else							/* not windows ... give up */
	return -1;
#endif
#endif
}


static struct rusage Save_r;
static struct timeval Save_t;

void
ResetUsage(void)
{
	getrusage(RUSAGE_SELF, &Save_r);
	gettimeofday(&Save_t, NULL);
}

void
ShowUsage(const char *title)
{
	StringInfoData str;
	struct timeval user,
				sys;
	struct timeval elapse_t;
	struct rusage r;

	getrusage(RUSAGE_SELF, &r);
	gettimeofday(&elapse_t, NULL);
	memcpy((char *) &user, (char *) &r.ru_utime, sizeof(user));
	memcpy((char *) &sys, (char *) &r.ru_stime, sizeof(sys));
	if (elapse_t.tv_usec < Save_t.tv_usec)
	{
		elapse_t.tv_sec--;
		elapse_t.tv_usec += 1000000;
	}
	if (r.ru_utime.tv_usec < Save_r.ru_utime.tv_usec)
	{
		r.ru_utime.tv_sec--;
		r.ru_utime.tv_usec += 1000000;
	}
	if (r.ru_stime.tv_usec < Save_r.ru_stime.tv_usec)
	{
		r.ru_stime.tv_sec--;
		r.ru_stime.tv_usec += 1000000;
	}

	/*
	 * The only stats we don't show here are ixrss, idrss, isrss.  It takes
	 * some work to interpret them, and most platforms don't fill them in.
	 */
	initStringInfo(&str);

	appendStringInfoString(&str, "! system usage stats:\n");
	appendStringInfo(&str,
					 "!\t%ld.%06ld s user, %ld.%06ld s system, %ld.%06ld s elapsed\n",
					 (long) (r.ru_utime.tv_sec - Save_r.ru_utime.tv_sec),
					 (long) (r.ru_utime.tv_usec - Save_r.ru_utime.tv_usec),
					 (long) (r.ru_stime.tv_sec - Save_r.ru_stime.tv_sec),
					 (long) (r.ru_stime.tv_usec - Save_r.ru_stime.tv_usec),
					 (long) (elapse_t.tv_sec - Save_t.tv_sec),
					 (long) (elapse_t.tv_usec - Save_t.tv_usec));
	appendStringInfo(&str,
					 "!\t[%ld.%06ld s user, %ld.%06ld s system total]\n",
					 (long) user.tv_sec,
					 (long) user.tv_usec,
					 (long) sys.tv_sec,
					 (long) sys.tv_usec);
#if defined(HAVE_GETRUSAGE)
	appendStringInfo(&str,
					 "!\t%ld kB max resident size\n",
#if defined(__darwin__)
	/* in bytes on macOS */
					 r.ru_maxrss / 1024
#else
	/* in kilobytes on most other platforms */
					 r.ru_maxrss
#endif
		);
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] filesystem blocks in/out\n",
					 r.ru_inblock - Save_r.ru_inblock,
	/* they only drink coffee at dec */
					 r.ru_oublock - Save_r.ru_oublock,
					 r.ru_inblock, r.ru_oublock);
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] page faults/reclaims, %ld [%ld] swaps\n",
					 r.ru_majflt - Save_r.ru_majflt,
					 r.ru_minflt - Save_r.ru_minflt,
					 r.ru_majflt, r.ru_minflt,
					 r.ru_nswap - Save_r.ru_nswap,
					 r.ru_nswap);
	appendStringInfo(&str,
					 "!\t%ld [%ld] signals rcvd, %ld/%ld [%ld/%ld] messages rcvd/sent\n",
					 r.ru_nsignals - Save_r.ru_nsignals,
					 r.ru_nsignals,
					 r.ru_msgrcv - Save_r.ru_msgrcv,
					 r.ru_msgsnd - Save_r.ru_msgsnd,
					 r.ru_msgrcv, r.ru_msgsnd);
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] voluntary/involuntary context switches\n",
					 r.ru_nvcsw - Save_r.ru_nvcsw,
					 r.ru_nivcsw - Save_r.ru_nivcsw,
					 r.ru_nvcsw, r.ru_nivcsw);
#endif							/* HAVE_GETRUSAGE */

	/* remove trailing newline */
	if (str.data[str.len - 1] == '\n')
		str.data[--str.len] = '\0';

	ereport(LOG,
			(errmsg_internal("%s", title),
			 errdetail_internal("%s", str.data)));

	pfree(str.data);
}

/*
 * on_proc_exit handler to log end of session
 */
static void
log_disconnections(int code, Datum arg pg_attribute_unused())
{
	Port	   *port = MyProcPort;
	long		secs;
	int			usecs;
	int			msecs;
	int			hours,
				minutes,
				seconds;

	TimestampDifference(MyStartTimestamp,
						GetCurrentTimestamp(),
						&secs, &usecs);
	msecs = usecs / 1000;

	hours = secs / SECS_PER_HOUR;
	secs %= SECS_PER_HOUR;
	minutes = secs / SECS_PER_MINUTE;
	seconds = secs % SECS_PER_MINUTE;

	ereport(LOG,
			(errmsg("disconnection: session time: %d:%02d:%02d.%03d "
					"user=%s database=%s host=%s%s%s",
					hours, minutes, seconds, msecs,
					port->user_name, port->database_name, port->remote_host,
					port->remote_port[0] ? " port=" : "", port->remote_port)));
}

/*
 * Start statement timeout timer, if enabled.
 *
 * If there's already a timeout running, don't restart the timer.  That
 * enables compromises between accuracy of timeouts and cost of starting a
 * timeout.
 */
static void
enable_statement_timeout(void)
{
	/* must be within an xact */
	Assert(xact_started);

	/* always disable statement timeout in QE */
	if (StatementTimeout > 0 && Gp_role != GP_ROLE_EXECUTE)
	{
		if (!get_timeout_active(STATEMENT_TIMEOUT))
			enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
	}
	else
	{
		if (get_timeout_active(STATEMENT_TIMEOUT))
			disable_timeout(STATEMENT_TIMEOUT, false);
	}
}

/*
 * Disable statement timeout, if active.
 */
static void
disable_statement_timeout(void)
{
	if (get_timeout_active(STATEMENT_TIMEOUT))
		disable_timeout(STATEMENT_TIMEOUT, false);
}

/*
 * GPDB: Enable the IdleGangTimeoutHandler to disconnect and destroy idle
 * cdbgang processes.
 */
void
enable_client_wait_timeout_interrupt(void)
{
	if (DoingCommandRead)
		EnableClientWaitTimeoutInterrupt();
}

/*
 * GPDB: Disable the IdleGangTimeoutHandler to prevent disconnecting and
 * destroying idle cdbgang processes.
 */
void
disable_client_wait_timeout_interrupt(void)
{
	if (DoingCommandRead)
		DisableClientWaitTimeoutInterrupt();
}

/*
 * Whether request on cancel or termination have arrived?
 */
inline bool
CancelRequested() {
	return InterruptPending && (ProcDiePending || QueryCancelPending);
}
