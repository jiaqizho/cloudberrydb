//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CCTEInfo.cpp
//
//	@doc:
//		Information about CTEs in a query
//---------------------------------------------------------------------------

#include "gpopt/base/CCTEInfo.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/operators/CExpressionPreprocessor.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalCTEProducer.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::CCTEInfoEntry
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCTEInfo::CCTEInfoEntry::CCTEInfoEntry(CMemoryPool *mp,
									   CExpression *pexprCTEProducer)
	: m_mp(mp),
	  m_pexprCTEProducer(pexprCTEProducer),
	  m_pexprCTEConsumer(nullptr),
	  m_phmcrulConsumers(nullptr),
	  m_fUsed(true),
	  m_hasOuterReferences(false)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexprCTEProducer);

	m_pexprCTEConsumer = GPOS_NEW(mp) CExpressionArray(mp);
	m_phmcrulConsumers = GPOS_NEW(mp) ColRefToUlongMap(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::CCTEInfoEntry
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCTEInfo::CCTEInfoEntry::CCTEInfoEntry(CMemoryPool *mp,
									   CExpression *pexprCTEProducer,
									   BOOL fUsed)
	: m_mp(mp),
	  m_pexprCTEProducer(pexprCTEProducer),
	  m_pexprCTEConsumer(nullptr),
	  m_phmcrulConsumers(nullptr),
	  m_fUsed(fUsed),
	  m_hasOuterReferences(false)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexprCTEProducer);

	m_pexprCTEConsumer = GPOS_NEW(mp) CExpressionArray(mp);
	m_phmcrulConsumers = GPOS_NEW(mp) ColRefToUlongMap(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::~CCTEInfoEntry
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCTEInfo::CCTEInfoEntry::~CCTEInfoEntry()
{
	m_pexprCTEProducer->Release();
	m_pexprCTEConsumer->Release();
	m_phmcrulConsumers->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::AddConsumerCols
//
//	@doc:
//		Add given consumer to the consumers array
//
//---------------------------------------------------------------------------
void CCTEInfo::CCTEInfoEntry::AddConsumer(CExpression * consumer)
{
	GPOS_ASSERT(consumer->Pop() && consumer->Pop()->Eopid() 
		== COperator::EopLogicalCTEConsumer);
	m_pexprCTEConsumer->Append(consumer);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::AddConsumerCols
//
//	@doc:
//		Add given columns to consumers column map
//
//---------------------------------------------------------------------------
void
CCTEInfo::CCTEInfoEntry::AddConsumerCols(CColRefArray *colref_array)
{
	GPOS_ASSERT(nullptr != colref_array);

	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		if (nullptr == m_phmcrulConsumers->Find(colref))
		{
			BOOL fSuccess GPOS_ASSERTS_ONLY =
				m_phmcrulConsumers->Insert(colref, GPOS_NEW(m_mp) ULONG(ul));
			GPOS_ASSERT(fSuccess);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::UlConsumerColPos
//
//	@doc:
//		Return position of given consumer column,
//		return gpos::ulong_max if column is not found in local map
//
//---------------------------------------------------------------------------
ULONG
CCTEInfo::CCTEInfoEntry::UlConsumerColPos(CColRef *colref)
{
	GPOS_ASSERT(nullptr != colref);

	ULONG *pul = m_phmcrulConsumers->Find(colref);
	if (nullptr == pul)
	{
		return gpos::ulong_max;
	}

	return *pul;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfoEntry::UlCTEId
//
//	@doc:
//		CTE id
//
//---------------------------------------------------------------------------
ULONG
CCTEInfo::CCTEInfoEntry::UlCTEId() const
{
	return CLogicalCTEProducer::PopConvert(m_pexprCTEProducer->Pop())
		->UlCTEId();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::CCTEInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCTEInfo::CCTEInfo(CMemoryPool *mp)
	: m_mp(mp),
	  m_phmulcteinfoentry(nullptr),
	  m_ulNextCTEId(0),
	  m_fEnableInlining(true),
	  m_phmulprodconsmap(nullptr),
	  m_phmcidcrCTE(nullptr),
	  m_phmpidcrsCTE(nullptr)
{
	GPOS_ASSERT(nullptr != mp);
	m_phmulcteinfoentry = GPOS_NEW(m_mp) UlongToCTEInfoEntryMap(m_mp);
	m_phmulprodconsmap = GPOS_NEW(m_mp) UlongToProducerConsumerMap(m_mp);
	m_phmcidcrCTE = GPOS_NEW(m_mp) UlongToColRefMap(m_mp);
	m_phmpidcrsCTE = GPOS_NEW(m_mp) UlongToColRefArrayMap(m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::~CCTEInfo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCTEInfo::~CCTEInfo()
{
	CRefCount::SafeRelease(m_phmulcteinfoentry);
	CRefCount::SafeRelease(m_phmulprodconsmap);
	CRefCount::SafeRelease(m_phmcidcrCTE);
	CRefCount::SafeRelease(m_phmpidcrsCTE);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::PreprocessCTEProducer
//
//	@doc:
//		Preprocess CTE producer expression
//
//---------------------------------------------------------------------------
CExpression *
CCTEInfo::PexprPreprocessCTEProducer(const CExpression *pexprCTEProducer)
{
	GPOS_ASSERT(nullptr != pexprCTEProducer);

	CExpression *pexprProducerChild = (*pexprCTEProducer)[0];

	// get cte output cols for preprocessing use
	CColRefSet *pcrsOutput =
		CLogicalCTEProducer::PopConvert(pexprCTEProducer->Pop())
			->DeriveOutputColumns();

	CExpression *pexprChildPreprocessed =
		CExpressionPreprocessor::PexprPreprocess(m_mp, pexprProducerChild,
												 pcrsOutput);

	COperator *pop = pexprCTEProducer->Pop();
	pop->AddRef();

	CExpression *pexprProducerPreprocessed =
		GPOS_NEW(m_mp) CExpression(m_mp, pop, pexprChildPreprocessed);

	pexprProducerPreprocessed->ResetStats();
	InitDefaultStats(pexprProducerPreprocessed);

	return pexprProducerPreprocessed;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::AddCTEProducer
//
//	@doc:
//		Add CTE producer to hashmap
//
//---------------------------------------------------------------------------
void
CCTEInfo::AddCTEProducer(CExpression *pexprCTEProducer)
{
	CExpression *pexprProducerToAdd =
		PexprPreprocessCTEProducer(pexprCTEProducer);

	COperator *pop = pexprCTEProducer->Pop();
	ULONG ulCTEId = CLogicalCTEProducer::PopConvert(pop)->UlCTEId();

	BOOL fInserted GPOS_ASSERTS_ONLY = m_phmulcteinfoentry->Insert(
		GPOS_NEW(m_mp) ULONG(ulCTEId),
		GPOS_NEW(m_mp) CCTEInfoEntry(m_mp, pexprProducerToAdd));
	GPOS_ASSERT(fInserted);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::AddCTEProducer
//
//	@doc:
//		Add CTE producer to hashmap
//
//---------------------------------------------------------------------------
void
CCTEInfo::AddCTEConsumer(CExpression *pexprCTEConsumer)
{
	COperator *pop = pexprCTEConsumer->Pop();
	ULONG ulCTEId = CLogicalCTEConsumer::PopConvert(pop)->UlCTEId();

	CCTEInfoEntry *infoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	// must exist
	GPOS_ASSERT(infoentry);
	pexprCTEConsumer->AddRef();
	infoentry->AddConsumer(pexprCTEConsumer);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::ReplaceCTEProducer
//
//	@doc:
//		Replace cte producer with given expression
//
//---------------------------------------------------------------------------
void
CCTEInfo::ReplaceCTEProducer(CExpression *pexprCTEProducer)
{
	COperator *pop = pexprCTEProducer->Pop();
	ULONG ulCTEId = CLogicalCTEProducer::PopConvert(pop)->UlCTEId();

	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

#ifdef GPOS_DBUG
	CExpression *pexprCTEProducerOld = pcteinfoentry->PexprProducer();
	COperator *popCTEProducerOld = pexprCTEProducerOld->Pop();
	GPOS_ASSERT(ulCTEId ==
				CLogicalCTEProducer::PopConvert(popCTEProducerOld)->UlCTEId());
#endif	// GPOS_DEBUG

	CExpression *pexprCTEProducerNew =
		PexprPreprocessCTEProducer(pexprCTEProducer);

	BOOL fReplaced GPOS_ASSERTS_ONLY = m_phmulcteinfoentry->Replace(
		&ulCTEId, GPOS_NEW(m_mp) CCTEInfoEntry(m_mp, pexprCTEProducerNew,
											   pcteinfoentry->FUsed()));
	GPOS_ASSERT(fReplaced);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::InitDefaultStats
//
//	@doc:
//		Initialize default statistics for a given CTE Producer
//
//---------------------------------------------------------------------------
void
CCTEInfo::InitDefaultStats(CExpression *pexprCTEProducer)
{
	// Generate statistics with empty requirement. This handles cases when
	// the CTE is a N-Ary join that will require statistics calculation

	CReqdPropRelational *prprel =
		GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(m_mp));
	(void) pexprCTEProducer->PstatsDerive(prprel, nullptr /* stats_ctxt */);

	// cleanup
	prprel->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::DeriveProducerStats
//
//	@doc:
//		Derive the statistics on the CTE producer
//
//---------------------------------------------------------------------------
void
CCTEInfo::DeriveProducerStats(CLogicalCTEConsumer *popConsumer,
							  CColRefSet *pcrsStat)
{
	const ULONG ulCTEId = popConsumer->UlCTEId();

	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

	CExpression *pexprCTEProducer = pcteinfoentry->PexprProducer();

	// Given the subset of CTE consumer columns needed for statistics derivation,
	// compute its corresponding set of columns in the CTE Producer
	CColRefSet *pcrsCTEProducer =
		CUtils::PcrsCTEProducerColumns(m_mp, pcrsStat, popConsumer);
	GPOS_ASSERT(pcrsStat->Size() == pcrsCTEProducer->Size());

	CReqdPropRelational *prprel =
		GPOS_NEW(m_mp) CReqdPropRelational(pcrsCTEProducer);
	(void) pexprCTEProducer->PstatsDerive(prprel, nullptr /* stats_ctxt */);

	// cleanup
	prprel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::PexprCTEProducer
//
//	@doc:
//		Return the logical cte producer with given id
//
//---------------------------------------------------------------------------
CExpression *
CCTEInfo::PexprCTEProducer(ULONG ulCTEId) const
{
	const CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

	return pcteinfoentry->PexprProducer();
}

// logical cte consumer with given id
CExpressionArray *
CCTEInfo::PexprCTEConsumer(ULONG ulCTEId) const
{
	const CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

	return pcteinfoentry->PexprsConsumer();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::UlConsumersInParent
//
//	@doc:
//		Number of consumers of given CTE inside a given parent
//
//---------------------------------------------------------------------------
ULONG
CCTEInfo::UlConsumersInParent(ULONG ulConsumerId, ULONG ulParentId) const
{
	// get map of given parent
	const UlongToConsumerCounterMap *phmulconsumermap =
		m_phmulprodconsmap->Find(&ulParentId);
	if (nullptr == phmulconsumermap)
	{
		return 0;
	}

	// find counter of given consumer inside this map
	const SConsumerCounter *pconsumercounter =
		phmulconsumermap->Find(&ulConsumerId);
	if (nullptr == pconsumercounter)
	{
		return 0;
	}

	return pconsumercounter->UlCount();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::UlConsumers
//
//	@doc:
//		Return number of CTE consumers of given CTE
//
//---------------------------------------------------------------------------
ULONG
CCTEInfo::UlConsumers(ULONG ulCTEId) const
{
	// find consumers in main query
	ULONG ulConsumers = UlConsumersInParent(ulCTEId, gpos::ulong_max);

	// find consumers in other CTEs
	UlongToCTEInfoEntryMapIter hmulei(m_phmulcteinfoentry);
	while (hmulei.Advance())
	{
		const CCTEInfoEntry *pcteinfoentry = hmulei.Value();
		if (pcteinfoentry->FUsed())
		{
			ulConsumers +=
				UlConsumersInParent(ulCTEId, pcteinfoentry->UlCTEId());
		}
	}

	return ulConsumers;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::FUsed
//
//	@doc:
//		Check if given CTE is used
//
//---------------------------------------------------------------------------
BOOL
CCTEInfo::FUsed(ULONG ulCTEId) const
{
	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);
	return pcteinfoentry->FUsed();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::HasOuterReferences
//
//	@doc:
//		Check if given CTE has outer reference
//
//---------------------------------------------------------------------------
BOOL
CCTEInfo::HasOuterReferences(ULONG ulCTEId) const
{
	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);
	return pcteinfoentry->HasOuterReferences();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::SetHasOuterReferences
//
//	@doc:
//		Check if given CTE has outer reference
//
//---------------------------------------------------------------------------
void
CCTEInfo::SetHasOuterReferences(ULONG ulCTEId)
{
	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);
	pcteinfoentry->SetHasOuterReferences();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::IncrementConsumers
//
//	@doc:
//		Increment number of CTE consumers
//
//---------------------------------------------------------------------------
void
CCTEInfo::IncrementConsumers(ULONG ulConsumerId, ULONG ulParentCTEId)
{
	// get map of given parent
	UlongToConsumerCounterMap *phmulconsumermap =
		m_phmulprodconsmap->Find(&ulParentCTEId);
	if (nullptr == phmulconsumermap)
	{
		phmulconsumermap = GPOS_NEW(m_mp) UlongToConsumerCounterMap(m_mp);
		BOOL fInserted GPOS_ASSERTS_ONLY = m_phmulprodconsmap->Insert(
			GPOS_NEW(m_mp) ULONG(ulParentCTEId), phmulconsumermap);
		GPOS_ASSERT(fInserted);
	}

	// find counter of given consumer inside this map
	SConsumerCounter *pconsumercounter = phmulconsumermap->Find(&ulConsumerId);
	if (nullptr == pconsumercounter)
	{
		// no existing counter - start a new one
		BOOL fInserted GPOS_ASSERTS_ONLY = phmulconsumermap->Insert(
			GPOS_NEW(m_mp) ULONG(ulConsumerId),
			GPOS_NEW(m_mp) SConsumerCounter(ulConsumerId));
		GPOS_ASSERT(fInserted);
	}
	else
	{
		// counter already exists - just increment it
		pconsumercounter->Increment();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::PcterProducers
//
//	@doc:
//		Return a CTE requirement with all the producers as optional
//
//---------------------------------------------------------------------------
CCTEReq *
CCTEInfo::PcterProducers(CMemoryPool *mp) const
{
	CCTEReq *pcter = GPOS_NEW(mp) CCTEReq(mp);

	UlongToCTEInfoEntryMapIter hmulei(m_phmulcteinfoentry);
	while (hmulei.Advance())
	{
		const CCTEInfoEntry *pcteinfoentry = hmulei.Value();
		pcter->Insert(pcteinfoentry->UlCTEId(), CCTEMap::EctProducer,
					  false /*fRequired*/, nullptr /*pdpplan*/);
	}

	return pcter;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::PdrgPexpr
//
//	@doc:
//		Return an array of all stored CTE expressions
//
//---------------------------------------------------------------------------
CExpressionArray *
CCTEInfo::PdrgPexpr(CMemoryPool *mp) const
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	UlongToCTEInfoEntryMapIter hmulei(m_phmulcteinfoentry);
	while (hmulei.Advance())
	{
		CExpression *pexpr = hmulei.Value()->PexprProducer();
		pexpr->AddRef();
		pdrgpexpr->Append(pexpr);
	}

	return pdrgpexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::MapComputedToUsedCols
//
//	@doc:
//		Walk the producer expressions and add the mapping between computed
//		column and its used columns
//
//---------------------------------------------------------------------------
void
CCTEInfo::MapComputedToUsedCols(CColumnFactory *col_factory) const
{
	UlongToCTEInfoEntryMapIter hmulei(m_phmulcteinfoentry);
	while (hmulei.Advance())
	{
		CExpression *pexprProducer = hmulei.Value()->PexprProducer();
		GPOS_ASSERT(nullptr != pexprProducer);
		CQueryContext::MapComputedToUsedCols(col_factory, pexprProducer);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::AddConsumerCols
//
//	@doc:
//		Add given columns to consumers column map
//
//---------------------------------------------------------------------------
void
CCTEInfo::AddConsumerCols(ULONG ulCTEId, CColRefArray *colref_array)
{
	GPOS_ASSERT(nullptr != colref_array);

	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

	pcteinfoentry->AddConsumerCols(colref_array);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::UlConsumerColPos
//
//	@doc:
//		Return position of given consumer column in consumer output
//
//---------------------------------------------------------------------------
ULONG
CCTEInfo::UlConsumerColPos(ULONG ulCTEId, CColRef *colref)
{
	GPOS_ASSERT(nullptr != colref);

	CCTEInfoEntry *pcteinfoentry = m_phmulcteinfoentry->Find(&ulCTEId);
	GPOS_ASSERT(nullptr != pcteinfoentry);

	return pcteinfoentry->UlConsumerColPos(colref);
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::FindConsumersInParent
//
//	@doc:
//		Find all CTE consumers inside given parent, and push them to the given stack
//
//---------------------------------------------------------------------------
void
CCTEInfo::FindConsumersInParent(ULONG ulParentId, CBitSet *pbsUnusedConsumers,
								CStack<ULONG> *pstack)
{
	UlongToConsumerCounterMap *phmulconsumermap =
		m_phmulprodconsmap->Find(&ulParentId);
	if (nullptr == phmulconsumermap)
	{
		// no map found for given parent - there are no consumers inside it
		return;
	}

	UlongToConsumerCounterMapIter hmulci(phmulconsumermap);
	while (hmulci.Advance())
	{
		const SConsumerCounter *pconsumercounter = hmulci.Value();
		ULONG ulConsumerId = pconsumercounter->UlCTEId();
		if (pbsUnusedConsumers->Get(ulConsumerId))
		{
			pstack->Push(GPOS_NEW(m_mp) ULONG(ulConsumerId));
			pbsUnusedConsumers->ExchangeClear(ulConsumerId);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::MarkUnusedCTEs
//
//	@doc:
//		Mark unused CTEs
//
//---------------------------------------------------------------------------
void
CCTEInfo::MarkUnusedCTEs()
{
	CBitSet *pbsUnusedConsumers = GPOS_NEW(m_mp) CBitSet(m_mp);

	// start with all CTEs
	UlongToCTEInfoEntryMapIter hmulei(m_phmulcteinfoentry);
	while (hmulei.Advance())
	{
		const CCTEInfoEntry *pcteinfoentry = hmulei.Value();
		pbsUnusedConsumers->ExchangeSet(pcteinfoentry->UlCTEId());
	}

	// start with the main query and find out which CTEs are used there
	CStack<ULONG> stack(m_mp);
	FindConsumersInParent(gpos::ulong_max, pbsUnusedConsumers, &stack);

	// repeatedly find CTEs that are used in these CTEs
	while (!stack.IsEmpty())
	{
		// get one CTE id from list, and see which consumers are inside this CTE
		ULONG *pulCTEId = stack.Pop();
		FindConsumersInParent(*pulCTEId, pbsUnusedConsumers, &stack);
		GPOS_DELETE(pulCTEId);
	}

	// now the only CTEs remaining in the bitset are the unused ones. mark them as such
	UlongToCTEInfoEntryMapIter hmulei2(m_phmulcteinfoentry);
	while (hmulei2.Advance())
	{
		CCTEInfoEntry *pcteinfoentry =
			const_cast<CCTEInfoEntry *>(hmulei2.Value());
		if (pbsUnusedConsumers->Get(pcteinfoentry->UlCTEId()))
		{
			pcteinfoentry->MarkUnused();
		}
		else
		{
			pcteinfoentry->MarkUsed();
		}
	}

	pbsUnusedConsumers->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEInfo::PhmulcrConsumerToProducer
//
//	@doc:
//		Return a map from Id's of consumer columns in the given column set
//		to their corresponding producer columns in the given column array
//
//---------------------------------------------------------------------------
UlongToColRefMap *
CCTEInfo::PhmulcrConsumerToProducer(
	CMemoryPool *mp, ULONG ulCTEId,
	CColRefSet *pcrs,			    // set of columns to check
	CColRefArray *pdrgpcrProducer,  // producer columns
	ULongPtrArray *pdrgpcrUnusedProducer // producer unused columns
)
{
	GPOS_ASSERT(nullptr != pcrs);
	GPOS_ASSERT(nullptr != pdrgpcrProducer);

	UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();
		// pruned colref no need do the mapping
		if (colref->GetUsage(true, true) != CColRef::EUsed) {
			continue;
		}
		ULONG ulPos = UlConsumerColPos(ulCTEId, colref);

		if (gpos::ulong_max != ulPos)
		{
			ULONG remapUlPos = ulPos;
			if (pdrgpcrUnusedProducer) {
				GPOS_ASSERT(ulPos < pdrgpcrUnusedProducer->Size());
				remapUlPos = ulPos - *(*pdrgpcrUnusedProducer)[ulPos];
				GPOS_ASSERT(remapUlPos < pdrgpcrProducer->Size());
			}

			CColRef *pcrProducer = (*pdrgpcrProducer)[remapUlPos];
			pcrProducer->MarkAsUsed();
			BOOL fSuccess GPOS_ASSERTS_ONLY = colref_mapping->Insert(
				GPOS_NEW(mp) ULONG(colref->Id()), pcrProducer);
			GPOS_ASSERT(fSuccess);
		}
	}
	return colref_mapping;
}


// EOF
