//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.cpp
//
//	@doc:
//		Implementation of index description
//---------------------------------------------------------------------------

#include "gpopt/metadata/CIndexDescriptor.h"

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CIndexDescriptor);

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::CIndexDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CIndexDescriptor::CIndexDescriptor(
	CMemoryPool *mp, IMDId *pmdidIndex, const CName &name,
	CColumnDescriptorArray *pdrgcoldescKeyCols,
	CColumnDescriptorArray *pdrgcoldescIncludedCols, BOOL is_clustered,
	IMDIndex::EmdindexType index_type)
	: m_pmdidIndex(pmdidIndex),
	  m_name(mp, name),
	  m_pdrgpcoldescKeyCols(pdrgcoldescKeyCols),
	  m_pdrgpcoldescIncludedCols(pdrgcoldescIncludedCols),
	  m_clustered(is_clustered),
	  m_index_type(index_type)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(pmdidIndex->IsValid());
	GPOS_ASSERT(nullptr != pdrgcoldescKeyCols);
	GPOS_ASSERT(nullptr != pdrgcoldescIncludedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::~CIndexDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CIndexDescriptor::~CIndexDescriptor()
{
	m_pmdidIndex->Release();

	m_pdrgpcoldescKeyCols->Release();
	m_pdrgpcoldescIncludedCols->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Keys
//
//	@doc:
//		number of key columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::Keys() const
{
	return m_pdrgpcoldescKeyCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlIncludedColumns
//
//	@doc:
//		Number of included columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlIncludedColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(nullptr != m_pdrgpcoldescIncludedCols);

	return m_pdrgpcoldescIncludedCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Pindexdesc
//
//	@doc:
//		Create the index descriptor from the table descriptor and index
//		information from the catalog
//
//---------------------------------------------------------------------------
CIndexDescriptor *
CIndexDescriptor::Pindexdesc(CMemoryPool *mp, const CTableDescriptor *ptabdesc,
							 const IMDIndex *pmdindex)
{
	CWStringConst strIndexName(mp, pmdindex->Mdname().GetMDName()->GetBuffer());

	CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();

	pmdindex->MDId()->AddRef();

	// array of index column descriptors
	CColumnDescriptorArray *pdrgcoldescKey =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);

	for (ULONG ul = 0; ul < pmdindex->Keys(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescKey->Append(pcoldesc);
	}

	// array of included column descriptors
	CColumnDescriptorArray *pdrgcoldescIncluded =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);
	for (ULONG ul = 0; ul < pmdindex->IncludedCols(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescIncluded->Append(pcoldesc);
	}


	// create the index descriptors
	CIndexDescriptor *pindexdesc = GPOS_NEW(mp) CIndexDescriptor(
		mp, pmdindex->MDId(), CName(&strIndexName), pdrgcoldescKey,
		pdrgcoldescIncluded, pmdindex->IsClustered(), pmdindex->IndexType());
	return pindexdesc;
}

BOOL
CIndexDescriptor::SupportsIndexOnlyScan(CTableDescriptor *ptabdesc) const
{
	// index only scan is not supported on GPDB 6 append-only tables.
	// CBDB_MERGE_FIXME: GIST/GIN will generated a wrong plan(with index only scan) 
	// util commit 42ba0dd2cf3c3564b96dae71aba894a886e25ed4([ORCA] Fix bug checking 
	// index_can_return() (#16575))
	// Temporarily disable indexonlyscan on non-btree indexes.
	// 
	// example:
	//	CREATE TABLE IF NOT EXISTS test_tsvector(t text,a tsvector);
	//  create index wowidx on test_tsvector using gist (a);
	//  analyze test_tsvector;
	//  set optimizer_enable_indexscan to off;
	//  set optimizer_enable_indexonlyscan to on;
	//  explain SELECT count(*) FROM test_tsvector WHERE a @@ 'pl <-> yh'; -- should not be Index Only Scan
	// 	                                              QUERY PLAN
	// ------------------------------------------------------------------------------------------------------
	//  Finalize Aggregate  (cost=0.00..6.06 rows=1 width=8)
	//    ->  Gather Motion 3:1  (slice1; segments: 3)  (cost=0.00..6.06 rows=1 width=8)
	//          ->  Partial Aggregate  (cost=0.00..6.06 rows=1 width=8)
	//                ->  Index Only Scan using wowidx on test_tsvector  (cost=0.00..6.01 rows=68 width=358)
	//                      Index Cond: (a @@ '''pl'' <-> ''yh'''::tsquery)
	//  Optimizer: Pivotal Optimizer (GPORCA)
	// (6 rows)
	// 
	return m_index_type == IMDIndex::EmdindBtree &&
		   !((ptabdesc->IsAORowOrColTable() ||
			  IMDRelation::ErelstorageMixedPartitioned ==
				  ptabdesc->RetrieveRelStorageType()) &&
			 ptabdesc->GetRelAOVersion() < IMDRelation::AORelationVersion_GP7);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CIndexDescriptor::OsPrint(IOstream &os) const
{
	m_name.OsPrint(os);
	os << ": (Keys :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescKeyCols,
							   m_pdrgpcoldescKeyCols->Size());
	os << "); ";

	os << "(Included Columns :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescIncludedCols,
							   m_pdrgpcoldescIncludedCols->Size());
	os << ")";

	os << " [ Clustered :";
	if (m_clustered)
	{
		os << "true";
	}
	else
	{
		os << "false";
	}
	os << " ]";
	return os;
}

// EOF
