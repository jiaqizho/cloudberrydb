/*-------------------------------------------------------------------------
 *
 * xactdesc.c
 *	  rmgr descriptor routines for access/transam/xact.c
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/xactdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "storage/dbdirnode.h"
#include "storage/sinval.h"
#include "storage/standbydefs.h"
#include "utils/timestamp.h"
#include "access/twophase_xlog.h"

/*
 * Parse the WAL format of an xact commit and abort records into an easier to
 * understand format.
 *
 * This routines are in xactdesc.c because they're accessed in backend (when
 * replaying WAL) and frontend (pg_waldump) code. This file is the only xact
 * specific one shared between both. They're complicated enough that
 * duplication would be bothersome.
 */

void
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed)
{
	char	   *data = ((char *) xlrec) + MinSizeOfXactCommit;

	memset(parsed, 0, sizeof(*parsed));

	parsed->xinfo = 0;			/* default is 0 */

	parsed->xact_time = xlrec->xact_time;
	parsed->tablespace_oid_to_delete_on_commit = xlrec->tablespace_oid_to_delete_on_commit;

	{
		xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;

		parsed->xinfo = xl_xinfo->xinfo;

		data += sizeof(xl_xact_xinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DBINFO)
	{
		xl_xact_dbinfo *xl_dbinfo = (xl_xact_dbinfo *) data;

		parsed->dbId = xl_dbinfo->dbId;
		parsed->tsId = xl_dbinfo->tsId;

		data += sizeof(xl_xact_dbinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;

		parsed->nsubxacts = xl_subxacts->nsubxacts;
		parsed->subxacts = xl_subxacts->subxacts;

		data += MinSizeOfXactSubxacts;
		data += parsed->nsubxacts * sizeof(TransactionId);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_RELFILENODES)
	{
		xl_xact_relfilenodes *xl_relfilenodes = (xl_xact_relfilenodes *) data;

		parsed->nrels = xl_relfilenodes->nrels;
		parsed->xnodes = xl_relfilenodes->xnodes;

		data += MinSizeOfXactRelfilenodes;
		data += xl_relfilenodes->nrels * sizeof(RelFileNodePendingDelete);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_INVALS)
	{
		xl_xact_invals *xl_invals = (xl_xact_invals *) data;

		parsed->nmsgs = xl_invals->nmsgs;
		parsed->msgs = xl_invals->msgs;

		data += MinSizeOfXactInvals;
		data += xl_invals->nmsgs * sizeof(SharedInvalidationMessage);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DELDBS)
	{
		xl_xact_deldbs *xl_deldbs = (xl_xact_deldbs *) data;

		parsed->ndeldbs = xl_deldbs->ndeldbs;
		parsed->deldbs = xl_deldbs->deldbs;

		data += MinSizeOfXactDelDbs;
		data += xl_deldbs->ndeldbs * sizeof(DbDirNode);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
	{
		xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;

		parsed->twophase_xid = xl_twophase->xid;

		data += sizeof(xl_xact_twophase);

		if (parsed->xinfo & XACT_XINFO_HAS_GID)
		{
			strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
			data += strlen(data) + 1;
		}
	}

	/* Note: no alignment is guaranteed after this point */

	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		xl_xact_origin xl_origin;

		/* no alignment is guaranteed, so copy onto stack */
		memcpy(&xl_origin, data, sizeof(xl_origin));

		parsed->origin_lsn = xl_origin.origin_lsn;
		parsed->origin_timestamp = xl_origin.origin_timestamp;

		data += sizeof(xl_xact_origin);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DISTRIB)
	{
		xl_xact_distrib *xl_distrib = (xl_xact_distrib *) data;

		parsed->distribXid = xl_distrib->distrib_xid;
		data += sizeof(xl_xact_distrib);
	}
}

void
ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed)
{
	char	   *data = ((char *) xlrec) + MinSizeOfXactAbort;

	memset(parsed, 0, sizeof(*parsed));

	parsed->xinfo = 0;			/* default is 0 */

	parsed->xact_time = xlrec->xact_time;
	parsed->tablespace_oid_to_delete_on_abort = xlrec->tablespace_oid_to_delete_on_abort;

	{
		xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;

		parsed->xinfo = xl_xinfo->xinfo;

		data += sizeof(xl_xact_xinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DBINFO)
	{
		xl_xact_dbinfo *xl_dbinfo = (xl_xact_dbinfo *) data;

		parsed->dbId = xl_dbinfo->dbId;
		parsed->tsId = xl_dbinfo->tsId;

		data += sizeof(xl_xact_dbinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;

		parsed->nsubxacts = xl_subxacts->nsubxacts;
		parsed->subxacts = xl_subxacts->subxacts;

		data += MinSizeOfXactSubxacts;
		data += parsed->nsubxacts * sizeof(TransactionId);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_RELFILENODES)
	{
		xl_xact_relfilenodes *xl_relfilenodes = (xl_xact_relfilenodes *) data;

		parsed->nrels = xl_relfilenodes->nrels;
		parsed->xnodes = xl_relfilenodes->xnodes;

		data += MinSizeOfXactRelfilenodes;
		data += xl_relfilenodes->nrels * sizeof(RelFileNodePendingDelete);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DELDBS)
	{
		xl_xact_deldbs *xl_deldbs = (xl_xact_deldbs *) data;

		parsed->ndeldbs = xl_deldbs->ndeldbs;
		parsed->deldbs = xl_deldbs->deldbs;

		data += MinSizeOfXactDelDbs;
		data += xl_deldbs->ndeldbs * sizeof(DbDirNode);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
	{
		xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;

		parsed->twophase_xid = xl_twophase->xid;

		data += sizeof(xl_xact_twophase);

		if (parsed->xinfo & XACT_XINFO_HAS_GID)
		{
			strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
			data += strlen(data) + 1;
		}
	}

	/* Note: no alignment is guaranteed after this point */

	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		xl_xact_origin xl_origin;

		/* no alignment is guaranteed, so copy onto stack */
		memcpy(&xl_origin, data, sizeof(xl_origin));

		parsed->origin_lsn = xl_origin.origin_lsn;
		parsed->origin_timestamp = xl_origin.origin_timestamp;

		data += sizeof(xl_xact_origin);
	}

}

/*
 * ParsePrepareRecord
 */
void
ParsePrepareRecord(uint8 info, xl_xact_prepare *xlrec, xl_xact_parsed_prepare *parsed)
{
	char	   *bufptr;

	bufptr = ((char *) xlrec) + MAXALIGN(sizeof(xl_xact_prepare));

	memset(parsed, 0, sizeof(*parsed));

	parsed->xact_time = xlrec->prepared_at;
	parsed->origin_lsn = xlrec->origin_lsn;
	parsed->origin_timestamp = xlrec->origin_timestamp;
	parsed->twophase_xid = xlrec->xid;
	parsed->dbId = xlrec->database;
	parsed->nsubxacts = xlrec->nsubxacts;
	parsed->nrels = xlrec->ncommitrels;
	parsed->nabortrels = xlrec->nabortrels;
	parsed->nmsgs = xlrec->ninvalmsgs;

	strncpy(parsed->twophase_gid, bufptr, xlrec->gidlen);
	bufptr += MAXALIGN(xlrec->gidlen);

	parsed->subxacts = (TransactionId *) bufptr;
	bufptr += MAXALIGN(xlrec->nsubxacts * sizeof(TransactionId));

	parsed->xnodes = (RelFileNodePendingDelete *) bufptr;
	bufptr += MAXALIGN(xlrec->ncommitrels * sizeof(RelFileNode));

	parsed->abortnodes = (RelFileNodePendingDelete *) bufptr;
	bufptr += MAXALIGN(xlrec->nabortrels * sizeof(RelFileNode));

	parsed->msgs = (SharedInvalidationMessage *) bufptr;
	bufptr += MAXALIGN(xlrec->ninvalmsgs * sizeof(SharedInvalidationMessage));
}

static void
xact_desc_relations(StringInfo buf, char *label, int nrels,
					RelFileNodePendingDelete *xnodes)
{
	int			i;

	if (nrels > 0)
	{
		appendStringInfo(buf, "; %s:", label);
		for (i = 0; i < nrels; i++)
		{
			BackendId  backendId = xnodes[i].isTempRelation ?
								  TempRelBackendId : InvalidBackendId;
			char	   *path = relpathbackend(xnodes[i].node,
											  backendId,
											  MAIN_FORKNUM);

			appendStringInfo(buf, " %s", path);
			pfree(path);
		}
	}
}

static void
xact_desc_subxacts(StringInfo buf, int nsubxacts, TransactionId *subxacts)
{
	int			i;

	if (nsubxacts > 0)
	{
		appendStringInfoString(buf, "; subxacts:");
		for (i = 0; i < nsubxacts; i++)
			appendStringInfo(buf, " %u", subxacts[i]);
	}
}

static void
xact_desc_commit(StringInfo buf, uint8 info, xl_xact_commit *xlrec, RepOriginId origin_id)
{
	xl_xact_parsed_commit parsed;

	ParseCommitRecord(info, xlrec, &parsed);

	/* If this is a prepared xact, show the xid of the original xact */
	if (TransactionIdIsValid(parsed.twophase_xid))
		appendStringInfo(buf, "%u: ", parsed.twophase_xid);

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));

	xact_desc_relations(buf, "rels", parsed.nrels, parsed.xnodes);
	xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts);

	standby_desc_invalidations(buf, parsed.nmsgs, parsed.msgs, parsed.dbId,
							   parsed.tsId,
							   XactCompletionRelcacheInitFileInval(parsed.xinfo));

	if (parsed.ndeldbs > 0)
	{
		appendStringInfoString(buf, "; deldbs:");
		for (int i = 0; i < parsed.ndeldbs; i++)
		{
			char *path =
					 GetDatabasePath(parsed.deldbs[i].database, parsed.deldbs[i].tablespace);

			appendStringInfo(buf, " %s", path);
			pfree(path);
		}
	}
	if (xlrec->tablespace_oid_to_delete_on_commit != InvalidOid)
		appendStringInfo(buf, "; tablespace_oid_to_delete_on_commit: %u", xlrec->tablespace_oid_to_delete_on_commit);

	if (XactCompletionForceSyncCommit(parsed.xinfo))
		appendStringInfoString(buf, "; sync");

	if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		appendStringInfo(buf, "; origin: node %u, lsn %X/%X, at %s",
						 origin_id,
						 LSN_FORMAT_ARGS(parsed.origin_lsn),
						 timestamptz_to_str(parsed.origin_timestamp));
	}

	if (parsed.distribXid != InvalidDistributedTransactionId)
	{
		appendStringInfo(buf, " gxid = "UINT64_FORMAT, parsed.distribXid);
	}
}

static void
xact_desc_distributed_commit(StringInfo buf, uint8 info, xl_xact_commit *xlrec, RepOriginId origin_id)
{
	xl_xact_parsed_commit parsed;

	ParseCommitRecord(info, xlrec, &parsed);

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
	appendStringInfo(buf, " gxid = "UINT64_FORMAT, parsed.distribXid);
}

static void
xact_desc_distributed_forget(StringInfo buf, xl_xact_distributed_forget *xlrec)
{
	appendStringInfo(buf, "gxid = "UINT64_FORMAT, xlrec->gxid);
}

static void
xact_desc_abort(StringInfo buf, uint8 info, xl_xact_abort *xlrec)
{
	xl_xact_parsed_abort parsed;

	ParseAbortRecord(info, xlrec, &parsed);

	/* If this is a prepared xact, show the xid of the original xact */
	if (TransactionIdIsValid(parsed.twophase_xid))
		appendStringInfo(buf, "%u: ", parsed.twophase_xid);

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));

	xact_desc_relations(buf, "rels", parsed.nrels, parsed.xnodes);
	xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts);

	if (parsed.ndeldbs > 0)
	{
		appendStringInfoString(buf, "; deldbs:");
		for (int i = 0; i < parsed.ndeldbs; i++)
		{
			char *path =
					 GetDatabasePath(parsed.deldbs[i].database, parsed.deldbs[i].tablespace);

			appendStringInfo(buf, " %s", path);
			pfree(path);
		}
	}
	if (xlrec->tablespace_oid_to_delete_on_abort != InvalidOid)
		appendStringInfo(buf, "; tablespace_oid_to_delete_on_abort: %u", xlrec->tablespace_oid_to_delete_on_abort);
}

static void
xact_desc_assignment(StringInfo buf, xl_xact_assignment *xlrec)
{
	int			i;

	appendStringInfoString(buf, "subxacts:");

	for (i = 0; i < xlrec->nsubxacts; i++)
		appendStringInfo(buf, " %u", xlrec->xsub[i]);
}

static void
xact_desc_prepare(StringInfo buf, uint8 info, TwoPhaseFileHeader *tpfh)
{
	const char *gid;
	Assert(info == XLOG_XACT_PREPARE);

	appendStringInfo(buf, "at = %s", timestamptz_to_str(tpfh->prepared_at));

	if (tpfh->gidlen > 0)
	{
		gid = (const char *)tpfh + MAXALIGN(sizeof(*tpfh));
		Assert(strlen(gid) == (tpfh->gidlen -1));

		appendStringInfo(buf, "; gid = %*s", tpfh->gidlen - 1, gid);
	}

	if (tpfh->tablespace_oid_to_delete_on_commit != InvalidOid)
		appendStringInfo(buf, "; tablespace_oid_to_delete_on_commit = %u", tpfh->tablespace_oid_to_delete_on_commit);
	if (tpfh->tablespace_oid_to_delete_on_abort != InvalidOid)
		appendStringInfo(buf, "; tablespace_oid_to_delete_on_abort = %u", tpfh->tablespace_oid_to_delete_on_abort);
}

void
xact_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) rec;

		xact_desc_commit(buf, XLogRecGetInfo(record), xlrec,
						 XLogRecGetOrigin(record));
	}
	else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) rec;

		xact_desc_abort(buf, XLogRecGetInfo(record), xlrec);
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		TwoPhaseFileHeader *tpfh = (TwoPhaseFileHeader*) rec;
		xact_desc_prepare(buf, XLogRecGetInfo(record), tpfh);
	}
	else if (info == XLOG_XACT_ASSIGNMENT)
	{
		xl_xact_assignment *xlrec = (xl_xact_assignment *) rec;

		/*
		 * Note that we ignore the WAL record's xid, since we're more
		 * interested in the top-level xid that issued the record and which
		 * xids are being reported here.
		 */
		appendStringInfo(buf, "xtop %u: ", xlrec->xtop);
		xact_desc_assignment(buf, xlrec);
	}
	else if (info == XLOG_XACT_DISTRIBUTED_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) rec;

		appendStringInfo(buf, "distributed commit ");
		xact_desc_distributed_commit(buf, XLogRecGetInfo(record), xlrec,
						 XLogRecGetOrigin(record));
	}
	else if (info == XLOG_XACT_DISTRIBUTED_FORGET)
	{
		xl_xact_distributed_forget *xlrec = (xl_xact_distributed_forget *) rec;

		appendStringInfo(buf, "distributed forget ");
		xact_desc_distributed_forget(buf, xlrec);
	}
	else if (info == XLOG_XACT_INVALIDATIONS)
	{
		xl_xact_invals *xlrec = (xl_xact_invals *) rec;

		standby_desc_invalidations(buf, xlrec->nmsgs, xlrec->msgs, InvalidOid,
								   InvalidOid, false);
	}
}

const char *
xact_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & XLOG_XACT_OPMASK)
	{
		case XLOG_XACT_COMMIT:
			id = "COMMIT";
			break;
		case XLOG_XACT_PREPARE:
			id = "PREPARE";
			break;
		case XLOG_XACT_ABORT:
			id = "ABORT";
			break;
		case XLOG_XACT_COMMIT_PREPARED:
			id = "COMMIT_PREPARED";
			break;
		case XLOG_XACT_ABORT_PREPARED:
			id = "ABORT_PREPARED";
			break;
		case XLOG_XACT_ASSIGNMENT:
			id = "ASSIGNMENT";
			break;
		case XLOG_XACT_DISTRIBUTED_COMMIT:
			id = "DISTRIBUTED_COMMIT";
			break;
		case XLOG_XACT_DISTRIBUTED_FORGET:
			id = "DISTRIBUTED_FORGET";
			break;
		case XLOG_XACT_INVALIDATIONS:
			id = "INVALIDATION";
			break;
	}

	return id;
}
