//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.cpp
//
//	@doc:
//		Routines to perform optimization related tasks using the gpos framework
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/utils/COptTasks.h"

extern "C" {
#include "cdb/cdbvars.h"
#include "optimizer/hints.h"
#include "optimizer/orca.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
}

#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/set.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpdbcost/CCostModelGPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/eval/CConstExprEvaluatorDXL.h"
#include "gpopt/exception.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/hints/CPlanHint.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/utils/CConstExprEvaluatorProxy.h"
#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/xforms/CXformFactory.h"
#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/exception.h"
#include "naucrates/init.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;
using namespace gpdbcost;

// size of error buffer
#define GPOPT_ERROR_BUFFER_SIZE 10 * 1024 * 1024

// definition of default AutoMemoryPool
#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

// default id for the source system
const CSystemId default_sysid(IMDId::EmdidGeneral, GPOS_WSZ_STR_LENGTH("GPDB"));

plan_hint_hook_type plan_hint_hook = nullptr;

// Check one-to-one mapping of row hint types
GPOS_CPL_ASSERT(CRowHint::RVT_ABSOLUTE ==
					(CRowHint::RowsValueType) RVT_ABSOLUTE,
				"CRowHint::RVT_ABSOLUTE must equal RVT_ABSOLUTE");
GPOS_CPL_ASSERT(CRowHint::RVT_ADD == (CRowHint::RowsValueType) RVT_ADD,
				"CRowHint::RVT_ADD must equal RVT_ADD");
GPOS_CPL_ASSERT(CRowHint::RVT_SUB == (CRowHint::RowsValueType) RVT_SUB,
				"CRowHint::RVT_SUB must equal RVT_SUB");
GPOS_CPL_ASSERT(CRowHint::RVT_MULTI == (CRowHint::RowsValueType) RVT_MULTI,
				"CRowHint::RVT_MULTI must equal RVT_MULTI");


//---------------------------------------------------------------------------
//	@function:
//		SOptContext::SOptContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
SOptContext::SOptContext() = default;

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::Free
//
//	@doc:
//		Free all members except those pointed to by either input or
//		output
//
//---------------------------------------------------------------------------
void
SOptContext::Free(SOptContext::EPin input, SOptContext::EPin output) const
{
	if (nullptr != m_query_dxl && epinQueryDXL != input &&
		epinQueryDXL != output)
	{
		gpdb::GPDBFree(m_query_dxl);
	}

	if (nullptr != m_query && epinQuery != input && epinQuery != output)
	{
		gpdb::GPDBFree(m_query);
	}

	if (nullptr != m_plan_dxl && epinPlanDXL != input && epinPlanDXL != output)
	{
		gpdb::GPDBFree(m_plan_dxl);
	}

	if (nullptr != m_plan_stmt && epinPlStmt != input && epinPlStmt != output)
	{
		gpdb::GPDBFree(m_plan_stmt);
	}

	if (nullptr != m_error_msg && epinErrorMsg != input &&
		epinErrorMsg != output)
	{
		gpdb::GPDBFree(m_error_msg);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::CloneErrorMsg
//
//	@doc:
//		Clone m_error_msg to given memory context. Return NULL if there is no
//		error message.
//
//---------------------------------------------------------------------------
CHAR *
SOptContext::CloneErrorMsg(MemoryContext context) const
{
	if (nullptr == context || nullptr == m_error_msg)
	{
		return nullptr;
	}
	return gpdb::MemCtxtStrdup(context, m_error_msg);
}


//---------------------------------------------------------------------------
//	@function:
//		SOptContext::Cast
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
SOptContext *
SOptContext::Cast(void *ptr)
{
	GPOS_ASSERT(nullptr != ptr);

	return reinterpret_cast<SOptContext *>(ptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CreateMultiByteCharStringFromWCString
//
//	@doc:
//		Return regular string from wide-character string
//
//---------------------------------------------------------------------------
CHAR *
COptTasks::CreateMultiByteCharStringFromWCString(const WCHAR *wcstr)
{
	GPOS_ASSERT(nullptr != wcstr);

	const ULONG input_len = GPOS_WSZ_LENGTH(wcstr);
	const ULONG wchar_size = GPOS_SIZEOF(WCHAR);
	const ULONG max_len = (input_len + 1) * wchar_size;

	CHAR *str = (CHAR *) gpdb::GPDBAlloc(max_len);

	gpos::clib::Wcstombs(str, const_cast<WCHAR *>(wcstr), max_len);
	str[max_len - 1] = '\0';

	return str;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Execute
//
//	@doc:
//		Execute a task using GPOS. TODO extend gpos to provide
//		this functionality
//
//---------------------------------------------------------------------------
void
COptTasks::Execute(void *(*func)(void *), void *func_arg)
{
	Assert(func);

	CHAR *err_buf = (CHAR *) palloc(GPOPT_ERROR_BUFFER_SIZE);
	err_buf[0] = '\0';

	// initialize DXL support
	InitDXL();

	bool abort_flag = false;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);

	gpos_exec_params params;
	params.func = func;
	params.arg = func_arg;
	params.stack_start = &params;
	params.error_buffer = err_buf;
	params.error_buffer_size = GPOPT_ERROR_BUFFER_SIZE;
	params.abort_requested = &abort_flag;

	// execute task and send log message to server log
	GPOS_TRY
	{
		(void) gpos_exec(&params);
	}
	GPOS_CATCH_EX(ex)
	{
		LogExceptionMessageAndDelete(err_buf);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	LogExceptionMessageAndDelete(err_buf);
}

void
COptTasks::LogExceptionMessageAndDelete(CHAR *err_buf)
{
	if ('\0' != err_buf[0])
	{
		elog(LOG, "%s",
			 CreateMultiByteCharStringFromWCString((WCHAR *) err_buf));
	}

	pfree(err_buf);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::ConvertToPlanStmtFromDXL
//
//	@doc:
//		Translate a DXL tree into a planned statement
//
//---------------------------------------------------------------------------
PlannedStmt *
COptTasks::ConvertToPlanStmtFromDXL(
	CMemoryPool *mp, CMDAccessor *md_accessor, const Query *orig_query,
	const CDXLNode *dxlnode, bool can_set_tag,
	DistributionHashOpsKind distribution_hashops)
{
	GPOS_ASSERT(nullptr != md_accessor);
	GPOS_ASSERT(nullptr != dxlnode);

	/*
	 * Since GPDB 7 (commit 0ae9004), plan node IDs start from 0 in GPDB.
	 * GPDB 6 and lower had plan node IDs starting from 0.
	 */
	CIdGenerator plan_id_generator(0 /* ulStartId */);
	CIdGenerator motion_id_generator(1 /* ulStartId */);
	CIdGenerator param_id_generator(0 /* ulStartId */);

	CContextDXLToPlStmt dxl_to_plan_stmt_ctxt(
		mp, &plan_id_generator, &motion_id_generator, &param_id_generator,
		distribution_hashops);

	// translate DXL -> PlannedStmt
	CTranslatorDXLToPlStmt dxl_to_plan_stmt_translator(
		mp, md_accessor, &dxl_to_plan_stmt_ctxt, gpdb::GetGPSegmentCount());
	return dxl_to_plan_stmt_translator.GetPlannedStmtFromDXL(
		dxlnode, orig_query, can_set_tag);
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::LoadSearchStrategy
//
//	@doc:
//		Load search strategy from given file
//
//---------------------------------------------------------------------------
CSearchStageArray *
COptTasks::LoadSearchStrategy(CMemoryPool *mp, char *path)
{
	CSearchStageArray *search_strategy_arr = nullptr;
	CParseHandlerDXL *dxl_parse_handler = nullptr;

	GPOS_TRY
	{
		if (nullptr != path && strlen(path) != 0)
		{
			dxl_parse_handler =
				CDXLUtils::GetParseHandlerForDXLFile(mp, path, nullptr);
			if (nullptr != dxl_parse_handler)
			{
				elog(DEBUG2, "\n[OPT]: Using search strategy in (%s)", path);

				search_strategy_arr = dxl_parse_handler->GetSearchStageArray();
				search_strategy_arr->AddRef();
			}
		}
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			GPOS_RETHROW(ex);
		}
		elog(DEBUG2, "\n[OPT]: Using default search strategy");
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	if (dxl_parse_handler)
		GPOS_DELETE(dxl_parse_handler);

	return search_strategy_arr;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::CreateOptimizerConfig
//
//	@doc:
//		Create the optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig *
COptTasks::CreateOptimizerConfig(CMemoryPool *mp, ICostModel *cost_model,
								 CPlanHint *plan_hints)
{
	// get chosen plan number, cost threshold
	ULLONG plan_id = (ULLONG) optimizer_plan_id;
	ULLONG num_samples = (ULLONG) optimizer_samples_number;
	DOUBLE cost_threshold = (DOUBLE) optimizer_cost_threshold;

	DOUBLE damping_factor_filter = (DOUBLE) optimizer_damping_factor_filter;
	DOUBLE damping_factor_join = (DOUBLE) optimizer_damping_factor_join;
	DOUBLE damping_factor_groupby = (DOUBLE) optimizer_damping_factor_groupby;

	ULONG cte_inlining_cutoff = (ULONG) optimizer_cte_inlining_bound;
	ULONG join_arity_for_associativity_commutativity =
		(ULONG) optimizer_join_arity_for_associativity_commutativity;
	ULONG array_expansion_threshold =
		(ULONG) optimizer_array_expansion_threshold;
	ULONG join_order_threshold = (ULONG) optimizer_join_order_threshold;
	ULONG broadcast_threshold = (ULONG) optimizer_penalize_broadcast_threshold;
	ULONG push_group_by_below_setop_threshold =
		(ULONG) optimizer_push_group_by_below_setop_threshold;
	ULONG xform_bind_threshold = (ULONG) optimizer_xform_bind_threshold;
	ULONG skew_factor = (ULONG) optimizer_skew_factor;

	return GPOS_NEW(mp) COptimizerConfig(
		GPOS_NEW(mp)
			CEnumeratorConfig(mp, plan_id, num_samples, cost_threshold),
		GPOS_NEW(mp)
			CStatisticsConfig(mp, damping_factor_filter, damping_factor_join,
							  damping_factor_groupby, MAX_STATS_BUCKETS),
		GPOS_NEW(mp) CCTEConfig(cte_inlining_cutoff), cost_model,
		GPOS_NEW(mp)
			CHint(join_arity_for_associativity_commutativity,
				  array_expansion_threshold, join_order_threshold,
				  broadcast_threshold,
				  false, /* don't create Assert nodes for constraints, we'll
								      * enforce them ourselves in the executor */
				  push_group_by_below_setop_threshold, xform_bind_threshold,
				  skew_factor),
		plan_hints,
		GPOS_NEW(mp) CWindowOids(mp, OID(F_ROW_NUMBER), OID(F_RANK_), OID(F_DENSE_RANK_)));
}

//---------------------------------------------------------------------------
//		@function:
//			COptTasks::SetCostModelParams
//
//      @doc:
//			Set cost model parameters
//
//---------------------------------------------------------------------------
void
COptTasks::SetCostModelParams(ICostModel *cost_model)
{
	GPOS_ASSERT(nullptr != cost_model);

	if (optimizer_nestloop_factor > 1.0)
	{
		// change NLJ cost factor
		ICostModelParams::SCostParam *cost_param =
			cost_model->GetCostModelParams()->PcpLookup(
				CCostModelParamsGPDB::EcpNLJFactor);
		CDouble nlj_factor(optimizer_nestloop_factor);
		cost_model->GetCostModelParams()->SetParam(
			cost_param->Id(), nlj_factor, nlj_factor - 0.5, nlj_factor + 0.5);
	}

	if (optimizer_sort_factor > 1.0 || optimizer_sort_factor < 1.0)
	{
		// change sort cost factor
		ICostModelParams::SCostParam *cost_param =
			cost_model->GetCostModelParams()->PcpLookup(
				CCostModelParamsGPDB::EcpSortTupWidthCostUnit);

		CDouble sort_factor(optimizer_sort_factor);
		cost_model->GetCostModelParams()->SetParam(
			cost_param->Id(), cost_param->Get() * optimizer_sort_factor,
			cost_param->GetLowerBoundVal() * optimizer_sort_factor,
			cost_param->GetUpperBoundVal() * optimizer_sort_factor);
	}
	if (optimizer_spilling_mem_threshold > 0.0) {
		ICostModelParams::SCostParam *cost_param =
				cost_model->GetCostModelParams()->PcpLookup(
					CCostModelParamsGPDB::EcpHJSpillingMemThreshold);

			CDouble spill_mem_threshold(optimizer_spilling_mem_threshold);
			cost_model->GetCostModelParams()->SetParam(
				cost_param->Id(), spill_mem_threshold,
				spill_mem_threshold - 0.0,
				spill_mem_threshold + 0.0);
	}

}


//---------------------------------------------------------------------------
//      @function:
//			COptTasks::GetCostModel
//
//      @doc:
//			Generate an instance of optimizer cost model
//
//---------------------------------------------------------------------------
ICostModel *
COptTasks::GetCostModel(CMemoryPool *mp, ULONG num_segments)
{
	ICostModel *cost_model = GPOS_NEW(mp) CCostModelGPDB(mp, num_segments);

	SetCostModelParams(cost_model);

	return cost_model;
}


//---------------------------------------------------------------------------
//	@function:
//		GenerateJoinNodes
//
//	@doc:
//		Converts OuterInnerRels into a JoinNode structure
//---------------------------------------------------------------------------
CJoinHint::JoinNode *
GenerateJoinNodes(CMemoryPool *mp, OuterInnerRels *outer_inner,
				  gpos::set<std::string> &aliases)
{
	if (nullptr == outer_inner)
	{
		return nullptr;
	}

	CJoinHint::JoinNode *pair = nullptr;

	if (outer_inner->relation != nullptr)
	{
		// Base case: processing outer_inner leaf node
		char *str_buffer =
			GPOS_NEW_ARRAY(mp, char, strlen(outer_inner->relation) + 1);
		memcpy(str_buffer, outer_inner->relation,
			   strlen(outer_inner->relation));
		str_buffer[strlen(outer_inner->relation)] = '\0';

		if (aliases.count(std::string(str_buffer)) > 0)
		{
			// invalid leading hint if alias is specified more than once
			return nullptr;
		}
		aliases.insert(std::string(str_buffer));

		CWStringConst *alias = GPOS_NEW(mp)
			CWStringConst(mp, str_buffer /*outer_inner->relation*/);

		GPOS_DELETE_ARRAY(str_buffer);

		pair = GPOS_NEW(mp) CJoinHint::JoinNode(alias);
	}
	else if (2 == list_length(outer_inner->outer_inner_pair))
	{
		// Recursive case: processing outer_inner branch node
		CJoinHint::JoinNode *left_joinnode = GenerateJoinNodes(
			mp,
			(OuterInnerRels *) lfirst(list_head(outer_inner->outer_inner_pair)),
			aliases);

		CJoinHint::JoinNode *right_joinnode = GenerateJoinNodes(
			mp,
			(OuterInnerRels *) lfirst(
				list_second_cell(outer_inner->outer_inner_pair)),
			aliases);

		if (nullptr == left_joinnode || nullptr == right_joinnode)
		{
			// bad input - outer_inner may be malformed
			CRefCount::SafeRelease(left_joinnode);
			CRefCount::SafeRelease(right_joinnode);
			return nullptr;
		}

		pair = GPOS_NEW(mp) CJoinHint::JoinNode(left_joinnode, right_joinnode,
												/*is_directed*/ true);
	}
	else
	{
		// bad user input
		//
		// Example: "Leading(((t1 t2)))"
	}

	return pair;
}


//---------------------------------------------------------------------------
//	@function:
//		GenerateJoinNodes
//
//	@doc:
//		Converts List of relation names into a JoinNode structure
//
//		Example:
//			List: [t1 t2 t3 t4]
//
//			JoinNode:       .
//			      x         .
//			     / \        .
//			    x  t4       .
//			   / \          .
//			  x   t3        .
//			 / \            .
//			t2 t1
//---------------------------------------------------------------------------
CJoinHint::JoinNode *
GenerateJoinNodes(CMemoryPool *mp, List *relations)
{
	CJoinHint::JoinNode *pair = nullptr;

	gpos::set<std::string> aliases(mp);

	ListCell *l;
	int count = 0;
	foreach_with_count(l, relations, count)
	{
		char *relation = (char *) lfirst(l);
		if (aliases.count(std::string(relation)) > 0)
		{
			pair->Release();
			// invalid leading hint if alias is specified more than once
			return nullptr;
		}
		aliases.insert(std::string(relation));

		CWStringConst *alias = GPOS_NEW(mp) CWStringConst(mp, relation);

		if (count == 0)
		{
			// As we traverse the input list relations, we build up the
			// JoinNode pair. The first element is an edge case because there
			// is no existing JoinNode pair yet.
			pair = GPOS_NEW(mp) CJoinHint::JoinNode(alias);
		}
		else
		{
			pair = GPOS_NEW(mp) CJoinHint::JoinNode(
				pair, GPOS_NEW(mp) CJoinHint::JoinNode(alias),
				/*is_directed*/ false);
		}
	}

	return pair;
}


//---------------------------------------------------------------------------
//      @function:
//			COptTasks::GetPlanHints
//
//      @doc:
//			Generate an instance of plan hints by parsing the query tree.
//
//---------------------------------------------------------------------------
CPlanHint *
COptTasks::GetPlanHints(CMemoryPool *mp, Query *query)
{
	HintState *hintstate = nullptr;
	if (plan_hint_hook != nullptr)
	{
		// Calling plan_hint_hook creates pg_hint_plan hint structures
		// (see optimizer/hints.h).
		hintstate = (HintState *) plan_hint_hook(query);
		if (hintstate != nullptr && hintstate->log_level > 0)
		{
			GPOS_SET_TRACE(EopttracePrintPgHintPlanLog);
		}
	}

	if (nullptr == hintstate)
	{
		return nullptr;
	}

	// Following code translates pg_hint_plan hint structures into ORCA hint
	// structures (see gpopt/hints/CPlanHint.h).

	CPlanHint *plan_hints = GPOS_NEW(mp) CPlanHint(mp);

	// Translate ScanMethodHint => CScanHint
	for (int ul = 0; ul < hintstate->num_hints[HINT_TYPE_SCAN_METHOD]; ul++)
	{
		ScanMethodHint *scan_hint =
			(ScanMethodHint *) hintstate->scan_hints[ul];
		if (nullptr == scan_hint->relname)
		{
			continue;
		}

		CScanHint::EType type = CScanHint::Sentinal;
		switch (scan_hint->base.hint_keyword)
		{
			case HINT_KEYWORD_SEQSCAN:
			{
				type = CScanHint::SeqScan;
				break;
			}
			case HINT_KEYWORD_NOSEQSCAN:
			{
				type = CScanHint::NoSeqScan;
				break;
			}
			case HINT_KEYWORD_INDEXSCAN:
			{
				type = CScanHint::IndexScan;
				break;
			}
			case HINT_KEYWORD_NOINDEXSCAN:
			{
				type = CScanHint::NoIndexScan;
				break;
			}
			case HINT_KEYWORD_INDEXONLYSCAN:
			{
				type = CScanHint::IndexOnlyScan;
				break;
			}
			case HINT_KEYWORD_NOINDEXONLYSCAN:
			{
				type = CScanHint::NoIndexOnlyScan;
				break;
			}
			case HINT_KEYWORD_BITMAPSCAN:
			{
				type = CScanHint::BitmapScan;
				break;
			}
			case HINT_KEYWORD_NOBITMAPSCAN:
			{
				type = CScanHint::NoBitmapScan;
				break;
			}
			default:
			{
				CWStringDynamic *error_message = GPOS_NEW(mp) CWStringDynamic(
					mp, GPOS_WSZ_LIT("Unsupported plan hint: "));
				error_message->AppendFormat(GPOS_WSZ_LIT("%s"),
											scan_hint->base.keyword);

				GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp,
						   error_message->GetBuffer());
				break;
			}
		}

		CScanHint *hint = plan_hints->GetScanHint(scan_hint->relname);
		if (nullptr == hint)
		{
			StringPtrArray *indexnames = GPOS_NEW(mp) StringPtrArray(mp);

			ListCell *l;
			foreach (l, scan_hint->indexnames)
			{
				char *indexname = (char *) lfirst(l);
				indexnames->Append(GPOS_NEW(mp) CWStringConst(mp, indexname));
			}

			hint = GPOS_NEW(mp) CScanHint(
				mp, GPOS_NEW(mp) CWStringConst(mp, scan_hint->relname),
				indexnames);
			plan_hints->AddHint(hint);
		}
		hint->AddType(type);
	}

	// Translate RowsHint => CRowHint

	for (int hint_index = 0; hint_index < hintstate->num_hints[HINT_TYPE_ROWS];
		 hint_index++)
	{
		RowsHint *row_hint = (RowsHint *) hintstate->rows_hints[hint_index];

		StringPtrArray *aliases = GPOS_NEW(mp) StringPtrArray(mp);

		for (int rel_index = 0; rel_index < row_hint->nrels; rel_index++)
		{
			aliases->Append(
				GPOS_NEW(mp) CWStringConst(mp, row_hint->relnames[rel_index]));
		}

		plan_hints->AddHint(GPOS_NEW(mp) CRowHint(
			mp, aliases, CDouble(row_hint->rows),
			(CRowHint::RowsValueType) row_hint->value_type));
	}


	// Translate LeadingHint => CJoinHint

	for (int hint_index = 0;
		 hint_index < hintstate->num_hints[HINT_TYPE_LEADING]; hint_index++)
	{
		LeadingHint *leading_hint =
			(LeadingHint *) hintstate->leading_hint[hint_index];
		CJoinHint::JoinNode *joinnode = nullptr;

		if (nullptr != leading_hint->outer_inner)
		{
			// is directed
			gpos::set<std::string> aliases(mp);
			joinnode =
				GenerateJoinNodes(mp, leading_hint->outer_inner, aliases);
			if (nullptr != joinnode)
			{
				plan_hints->AddHint(GPOS_NEW(mp) CJoinHint(mp, joinnode));
			}
		}
		else if (nullptr != leading_hint->relations)
		{
			// is directed-less
			joinnode = GenerateJoinNodes(mp, leading_hint->relations);
			if (nullptr != joinnode)
			{
				plan_hints->AddHint(GPOS_NEW(mp) CJoinHint(mp, joinnode));
			}
		}
	}

	for (int hint_index = 0;
		 hint_index < hintstate->num_hints[HINT_TYPE_JOIN_METHOD]; hint_index++)
	{
		JoinMethodHint *joinmethod_hint =
			(JoinMethodHint *) hintstate->join_hints[hint_index];
		StringPtrArray *aliasnames = GPOS_NEW(mp) StringPtrArray(mp);
		for (int relname_index = 0; relname_index < joinmethod_hint->nrels;
			 relname_index++)
		{
			aliasnames->Append(GPOS_NEW(mp) CWStringConst(
				mp, joinmethod_hint->relnames[relname_index]));
		}

		CJoinTypeHint::JoinType type = CJoinTypeHint::SENTINEL;
		switch (joinmethod_hint->base.hint_keyword)
		{
			case HINT_KEYWORD_NESTLOOP:
			{
				type = CJoinTypeHint::HINT_KEYWORD_NESTLOOP;
				break;
			}
			case HINT_KEYWORD_MERGEJOIN:
			{
				type = CJoinTypeHint::HINT_KEYWORD_MERGEJOIN;
				break;
			}
			case HINT_KEYWORD_HASHJOIN:
			{
				type = CJoinTypeHint::HINT_KEYWORD_HASHJOIN;
				break;
			}
			case HINT_KEYWORD_NONESTLOOP:
			{
				type = CJoinTypeHint::HINT_KEYWORD_NONESTLOOP;
				break;
			}
			case HINT_KEYWORD_NOMERGEJOIN:
			{
				type = CJoinTypeHint::HINT_KEYWORD_NOMERGEJOIN;
				break;
			}
			case HINT_KEYWORD_NOHASHJOIN:
			{
				type = CJoinTypeHint::HINT_KEYWORD_NOHASHJOIN;
				break;
			}
			default:
			{
				CWStringDynamic *error_message = GPOS_NEW(mp) CWStringDynamic(
					mp, GPOS_WSZ_LIT("Unsupported plan hint: "));
				error_message->AppendFormat(GPOS_WSZ_LIT("%s"),
											joinmethod_hint->base.keyword);

				GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp,
						   error_message->GetBuffer());
				break;
			}
		}
		CJoinTypeHint *hint = GPOS_NEW(mp) CJoinTypeHint(mp, type, aliasnames);
		plan_hints->AddHint(hint);
	}

	return plan_hints;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::OptimizeTask
//
//	@doc:
//		task that does the optimizes query to physical DXL
//
//---------------------------------------------------------------------------
void *
COptTasks::OptimizeTask(void *ptr)
{
	GPOS_ASSERT(nullptr != ptr);
	SOptContext *opt_ctxt = SOptContext::Cast(ptr);

	GPOS_ASSERT(nullptr != opt_ctxt->m_query);
	GPOS_ASSERT(nullptr == opt_ctxt->m_plan_dxl);
	GPOS_ASSERT(nullptr == opt_ctxt->m_plan_stmt);

	AUTO_MEM_POOL(amp);
	CMemoryPool *mp = amp.Pmp();

	// Does the metadatacache need to be reset?
	//
	// On the first call, before the cache has been initialized, we
	// don't care about the return value of MDCacheNeedsReset(). But
	// we need to call it anyway, to give it a chance to initialize
	// the invalidation mechanism.
	bool reset_mdcache = gpdb::MDCacheNeedsReset();

	// initialize metadata cache, or purge if needed, or change size if requested
	if (!CMDCache::FInitialized())
	{
		CMDCache::Init();
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}
	else if (reset_mdcache)
	{
		CMDCache::Reset();
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}
	else if (CMDCache::ULLGetCacheQuota() !=
			 (ULLONG) optimizer_mdcache_size * 1024L)
	{
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}


	// load search strategy
	CSearchStageArray *search_strategy_arr =
		LoadSearchStrategy(mp, optimizer_search_strategy_path);

	CBitSet *trace_flags = nullptr;
	CBitSet *enabled_trace_flags = nullptr;
	CBitSet *disabled_trace_flags = nullptr;
	CDXLNode *plan_dxl = nullptr;

	IMdIdArray *col_stats = nullptr;
	MdidHashSet *rel_stats = nullptr;

	GPOS_TRY
	{
		// set trace flags
		trace_flags = CConfigParamMapping::PackConfigParamInBitset(
			mp, CXform::ExfSentinel);
		SetTraceflags(mp, trace_flags, &enabled_trace_flags,
					  &disabled_trace_flags);

		// set up relcache MD provider
		CMDProviderRelcache *relcache_provider =
			GPOS_NEW(mp) CMDProviderRelcache();

		{
			// scope for MD accessor
			CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid,
							relcache_provider);

			ULONG num_segments = gpdb::GetGPSegmentCount();
			ULONG num_segments_for_costing = optimizer_segments;
			if (0 == num_segments_for_costing)
			{
				num_segments_for_costing = num_segments;
			}

			CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator;
			query_to_dxl_translator = CTranslatorQueryToDXL::QueryToDXLInstance(
				mp, &mda, (Query *) opt_ctxt->m_query);

			ICostModel *cost_model = GetCostModel(mp, num_segments_for_costing);
			CPlanHint *plan_hints = GetPlanHints(mp, opt_ctxt->m_query);
			COptimizerConfig *optimizer_config =
				CreateOptimizerConfig(mp, cost_model, plan_hints);
			CConstExprEvaluatorProxy expr_eval_proxy(mp, &mda);
			IConstExprEvaluator *expr_evaluator =
				GPOS_NEW(mp) CConstExprEvaluatorDXL(mp, &mda, &expr_eval_proxy);

			CDXLNode *query_dxl =
				query_to_dxl_translator->TranslateQueryToDXL();
			CDXLNodeArray *query_output_dxlnode_array =
				query_to_dxl_translator->GetQueryOutputCols();
			CDXLNodeArray *cte_dxlnode_array =
				query_to_dxl_translator->GetCTEs();
			GPOS_ASSERT(nullptr != query_output_dxlnode_array);

			BOOL is_master_only =
				!optimizer_enable_motions ||
				(!optimizer_enable_motions_masteronly_queries &&
				 !query_to_dxl_translator->HasDistributedTables());
			// See NoteDistributionPolicyOpclasses() in src/backend/gpopt/translate/CTranslatorQueryToDXL.cpp
			BOOL use_legacy_opfamilies =
				(query_to_dxl_translator->GetDistributionHashOpsKind() ==
				 DistrUseLegacyHashOps);
			CAutoTraceFlag atf1(EopttraceDisableMotions, is_master_only);
			CAutoTraceFlag atf2(EopttraceUseLegacyOpfamilies,
								use_legacy_opfamilies);

			plan_dxl = COptimizer::PdxlnOptimize(
				mp, &mda, query_dxl, query_output_dxlnode_array,
				cte_dxlnode_array, expr_evaluator, num_segments, gp_session_id,
				gp_command_count, search_strategy_arr, optimizer_config);

			if (opt_ctxt->m_should_serialize_plan_dxl)
			{
				// serialize DXL to xml
				CWStringDynamic plan_str(mp);
				COstreamString oss(&plan_str);
				CDXLUtils::SerializePlan(
					mp, oss, plan_dxl,
					optimizer_config->GetEnumeratorCfg()->GetPlanId(),
					optimizer_config->GetEnumeratorCfg()->GetPlanSpaceSize(),
					true /*serialize_header_footer*/, true /*indentation*/);
				opt_ctxt->m_plan_dxl =
					CreateMultiByteCharStringFromWCString(plan_str.GetBuffer());
			}

			// translate DXL->PlStmt only when needed
			if (opt_ctxt->m_should_generate_plan_stmt)
			{
				// always use opt_ctxt->m_query->can_set_tag as the query_to_dxl_translator->Pquery() is a mutated Query object
				// that may not have the correct can_set_tag
				opt_ctxt->m_plan_stmt =
					(PlannedStmt *) gpdb::CopyObject(ConvertToPlanStmtFromDXL(
						mp, &mda, opt_ctxt->m_query, plan_dxl,
						opt_ctxt->m_query->canSetTag,
						query_to_dxl_translator->GetDistributionHashOpsKind()));
			}

			CStatisticsConfig *stats_conf = optimizer_config->GetStatsConf();
			col_stats = GPOS_NEW(mp) IMdIdArray(mp);
			// CBDB_MERGE_FIXME: empty table after analyze still have no stats
			// cause CBDB can't tell empty or no stats
			stats_conf->CollectMissingStatsColumns(col_stats);

			rel_stats = GPOS_NEW(mp) MdidHashSet(mp);
			PrintMissingStatsWarning(mp, &mda, col_stats, rel_stats);

			rel_stats->Release();
			col_stats->Release();

			expr_evaluator->Release();
			query_dxl->Release();
			optimizer_config->Release();
			plan_dxl->Release();
		}
	}
	GPOS_CATCH_EX(ex)
	{
		ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
		CRefCount::SafeRelease(rel_stats);
		CRefCount::SafeRelease(col_stats);
		CRefCount::SafeRelease(enabled_trace_flags);
		CRefCount::SafeRelease(disabled_trace_flags);
		CRefCount::SafeRelease(trace_flags);
		CRefCount::SafeRelease(plan_dxl);
		CMDCache::Shutdown();

		IErrorContext *errctxt = CTask::Self()->GetErrCtxt();

		opt_ctxt->m_is_unexpected_failure = IsLoggableFailure(ex);
		opt_ctxt->m_error_msg =
			CreateMultiByteCharStringFromWCString(errctxt->GetErrorMsg());

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// cleanup
	ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
	CRefCount::SafeRelease(enabled_trace_flags);
	CRefCount::SafeRelease(disabled_trace_flags);
	CRefCount::SafeRelease(trace_flags);
	if (!optimizer_metadata_caching)
	{
		CMDCache::Shutdown();
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PrintMissingStatsWarning
//
//	@doc:
//		Print warning messages for columns with missing statistics
//
//---------------------------------------------------------------------------
void
COptTasks::PrintMissingStatsWarning(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMdIdArray *col_stats,
									MdidHashSet *rel_stats)
{
	GPOS_ASSERT(nullptr != md_accessor);
	GPOS_ASSERT(nullptr != col_stats);
	GPOS_ASSERT(nullptr != rel_stats);

	CWStringDynamic wcstr(mp);
	COstreamString oss(&wcstr);

	const ULONG num_missing_col_stats = col_stats->Size();
	for (ULONG ul = 0; ul < num_missing_col_stats; ul++)
	{
		IMDId *mdid = (*col_stats)[ul];
		CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);

		IMDId *rel_mdid = mdid_col_stats->GetRelMdId();
		const ULONG pos = mdid_col_stats->Position();
		const IMDRelation *rel = md_accessor->RetrieveRel(rel_mdid);

		if (IMDRelation::ErelstorageForeign != rel->RetrieveRelStorageType())
		{
			if (!rel_stats->Contains(rel_mdid))
			{
				if (0 != ul)
				{
					oss << ", ";
				}

				rel_mdid->AddRef();
				rel_stats->Insert(rel_mdid);
				oss << rel->Mdname().GetMDName()->GetBuffer();
			}

			CMDName mdname = rel->GetMdCol(pos)->Mdname();

			char msgbuf[NAMEDATALEN * 2 + 100];
			snprintf(msgbuf, sizeof(msgbuf),
					 "Missing statistics for column: %s.%s",
					 CreateMultiByteCharStringFromWCString(
						 rel->Mdname().GetMDName()->GetBuffer()),
					 CreateMultiByteCharStringFromWCString(
						 mdname.GetMDName()->GetBuffer()));
			GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, LOG, msgbuf, nullptr);
		}
	}

	if (0 < rel_stats->Size())
	{
		int length = NAMEDATALEN * rel_stats->Size() + 200;
		char msgbuf[length];
		snprintf(
			msgbuf, sizeof(msgbuf),
			"One or more columns in the following table(s) do not have statistics: %s",
			CreateMultiByteCharStringFromWCString(wcstr.GetBuffer()));
		GpdbEreport(
			ERRCODE_SUCCESSFUL_COMPLETION, NOTICE, msgbuf,
			"For non-partitioned tables, run analyze <table_name>(<column_list>)."
			" For partitioned tables, run analyze rootpartition <table_name>(<column_list>)."
			" See log for columns missing statistics.");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Optimize
//
//	@doc:
//		optimizes a query to physical DXL
//
//---------------------------------------------------------------------------
char *
COptTasks::Optimize(Query *query)
{
	Assert(query);

	SOptContext gpopt_context;
	gpopt_context.m_query = query;
	gpopt_context.m_should_serialize_plan_dxl = true;
	Execute(&OptimizeTask, &gpopt_context);

	// clean up context
	gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlanDXL);

	return gpopt_context.m_plan_dxl;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::GPOPTOptimizedPlan
//
//	@doc:
//		optimizes a query to plannedstmt
//
//---------------------------------------------------------------------------
PlannedStmt *
COptTasks::GPOPTOptimizedPlan(Query *query, SOptContext *gpopt_context)
{
	Assert(query);
	Assert(gpopt_context);

	gpopt_context->m_query = query;
	gpopt_context->m_should_generate_plan_stmt = true;
	Execute(&OptimizeTask, gpopt_context);
	return gpopt_context->m_plan_stmt;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SetXform
//
//	@doc:
//		Enable/Disable a given xform
//
//---------------------------------------------------------------------------
bool
COptTasks::SetXform(char *xform_str, bool should_disable)
{
	CXform *xform = CXformFactory::Pxff()->Pxf(xform_str);
	if (nullptr != xform)
	{
		optimizer_xforms[xform->Exfid()] = should_disable;

		return true;
	}

	return false;
}

// EOF
