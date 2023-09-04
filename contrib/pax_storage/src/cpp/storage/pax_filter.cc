#include "storage/pax_filter.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_stats.h"
#include "storage/proto/proto_wrappers.h"

namespace paxc {
static bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                          ScanKey *p_scan_keys, int *p_num_scan_keys) {
  ListCell *qual_cell;
  ScanKey scan_keys;
  int n_scan_keys;
  int j;
  TupleDesc desc;

  /* Allocate array for ScanKey structs: one per qual */
  n_scan_keys = list_length(quals);
  scan_keys = (ScanKey)palloc(n_scan_keys * sizeof(ScanKeyData));
  desc = rel->rd_att;
  Oid *opfamilies = (Oid *)palloc(sizeof(Oid) * desc->natts);
  for (auto i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    if (attr->attisdropped) {
      opfamilies[i] = 0;
      continue;
    }
    Oid opclass = GetDefaultOpClass(attr->atttypid, BRIN_AM_OID);
    if (!OidIsValid(opclass)) {
      opfamilies[i] = 0;
      continue;
    }
    opfamilies[i] = get_opclass_family(opclass);
  }

  j = 0;
  foreach (qual_cell, quals) {
    Expr *clause = (Expr *)lfirst(qual_cell);
    ScanKey this_scan_key = &scan_keys[j];
    Oid opno;              /* operator's OID */
    RegProcedure opfuncid; /* operator proc id used in scan */
    Oid opfamily;          /* opfamily of index column */
    int op_strategy;       /* operator's strategy number */
    Oid op_lefttype;       /* operator's declared input types */
    Oid op_righttype;
    Expr *leftop;        /* expr on lhs of operator */
    Expr *rightop;       /* expr on rhs ... */
    AttrNumber varattno; /* att number used in scan */
    int indnkeyatts;

    indnkeyatts = RelationGetNumberOfAttributes(rel);
    if (IsA(clause, OpExpr)) {
      /* indexkey op const or indexkey op expression */
      int flags = 0;
      Datum scanvalue;

      opno = ((OpExpr *)clause)->opno;
      opfuncid = ((OpExpr *)clause)->opfuncid;

      /*
       * leftop should be the index key Var, possibly relabeled
       */
      leftop = (Expr *)get_leftop(clause);

      if (leftop && IsA(leftop, RelabelType))
        leftop = ((RelabelType *)leftop)->arg;

      Assert(leftop != NULL);

      if (!IsA(leftop, Var)) goto ignore_clause;

      varattno = ((Var *)leftop)->varattno;
      if (varattno < 1 || varattno > indnkeyatts)
        elog(ERROR, "bogus index qualification");

      /*
       * We have to look up the operator's strategy number.  This
       * provides a cross-check that the operator does match the index.
       */
      opfamily = opfamilies[varattno - 1];
      if (!OidIsValid(opfamily)) goto ignore_clause;

      get_op_opfamily_properties(opno, opfamily, isorderby, &op_strategy,
                                 &op_lefttype, &op_righttype);

      if (isorderby) flags |= SK_ORDER_BY;

      /*
       * rightop is the constant or variable comparison value
       */
      rightop = (Expr *)get_rightop(clause);

      if (rightop && IsA(rightop, RelabelType))
        rightop = ((RelabelType *)rightop)->arg;

      Assert(rightop != NULL);

      if (IsA(rightop, Const)) {
        /* OK, simple constant comparison value */
        scanvalue = ((Const *)rightop)->constvalue;
        if (((Const *)rightop)->constisnull) flags |= SK_ISNULL;
      } else {
        // No support for runtime keys now
        goto ignore_clause;
      }

      /*
       * initialize the scan key's fields appropriately
       */
      ScanKeyEntryInitialize(this_scan_key, flags,
                             varattno,     /* attribute number to scan */
                             op_strategy,  /* op's strategy */
                             op_righttype, /* strategy subtype */
                             ((OpExpr *)clause)->inputcollid, /* collation */
                             opfuncid,   /* reg proc to use */
                             scanvalue); /* constant */
      j++;
    } else if (IsA(clause, NullTest)) {
      /* indexkey IS NULL or indexkey IS NOT NULL */
      auto ntest = reinterpret_cast<NullTest *>(clause);
      int flags;

      Assert(!isorderby);

      /*
       * argument should be the index key Var, possibly relabeled
       */
      leftop = ntest->arg;

      if (leftop && IsA(leftop, RelabelType))
        leftop = ((RelabelType *)leftop)->arg;

      Assert(leftop != NULL);

      if (!IsA(leftop, Var)) goto ignore_clause;

      varattno = ((Var *)leftop)->varattno;

      /*
       * initialize the scan key's fields appropriately
       */
      switch (ntest->nulltesttype) {
        case IS_NULL:
          flags = SK_ISNULL | SK_SEARCHNULL;
          break;
        case IS_NOT_NULL:
          flags = SK_ISNULL | SK_SEARCHNOTNULL;
          break;
        default:
          elog(ERROR, "unrecognized nulltesttype: %d",
               (int)ntest->nulltesttype);
          flags = 0; /* keep compiler quiet */
          break;
      }

      ScanKeyEntryInitialize(this_scan_key, flags,
                             varattno,        /* attribute number to scan */
                             InvalidStrategy, /* no strategy */
                             InvalidOid,      /* no strategy subtype */
                             InvalidOid,      /* no collation */
                             InvalidOid,      /* no reg proc for this */
                             (Datum)0);       /* constant */
      j++;
    } else {
      // not support other qual types yet
    }

  ignore_clause:
    continue;
  }
  pfree(opfamilies);

  /*
   * Return info to our caller.
   */
  if (j > 0) {
    *p_scan_keys = scan_keys;
    *p_num_scan_keys = j;
    return true;
  }
  return false;
}
}  // namespace paxc

namespace pax {

bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                   ScanKey *scan_keys, int *num_scan_keys) {
  CBDB_WRAP_START;
  {
    return paxc::BuildScanKeys(rel, quals, isorderby, scan_keys, num_scan_keys);
  }
  CBDB_WRAP_END;
}

PaxFilter::~PaxFilter() { delete[] proj_; }

std::pair<bool *, size_t> PaxFilter::GetColumnProjection() {
  return std::make_pair(proj_, proj_len_);
}

void PaxFilter::SetColumnProjection(bool *proj, size_t proj_len) {
  proj_ = proj;
  proj_len_ = proj_len;
}

void PaxFilter::SetScanKeys(ScanKey scan_keys, int num_scan_keys) {
  Assert(num_scan_keys_ == 0);

  if (num_scan_keys > 0) {
    scan_keys_ = scan_keys;
    num_scan_keys_ = num_scan_keys;
  }
}

static inline bool CheckNullKey(
    ScanKey scan_key, const ::pax::stats::ColumnStatisitcsInfo &column_stats) {
  // handle null test
  // SK_SEARCHNULL and SK_SEARCHNOTNULL must not co-exist with each other
  Assert(scan_key->sk_flags & SK_ISNULL);
  Assert((scan_key->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) !=
         (SK_SEARCHNULL | SK_SEARCHNOTNULL));

  if (scan_key->sk_flags & SK_SEARCHNULL) {
    // test: IS NULL
    if (!column_stats.hasnull()) return false;
  } else if (scan_key->sk_flags & SK_SEARCHNOTNULL) {
    // test: IS NOT NULL
    if (column_stats.allnull()) return false;
  } else {
    // Neither IS NULL nor IS NOT NULL was used; assume all indexable
    // operators are strict and thus return false with NULL value in
    // the scan key.
    return false;
  }
  return true;
}

static inline bool CheckProcid(const ::pax::stats::MinmaxStatistics &minmax,
                               StrategyNumber strategy, Oid procid) {
  switch (strategy) {
    case BTLessStrategyNumber:
      return minmax.proclt() == procid;
    case BTLessEqualStrategyNumber:
      return minmax.procle() == procid;
    case BTGreaterStrategyNumber:
      return minmax.procgt() == procid;
    case BTGreaterEqualStrategyNumber:
      return minmax.procge() == procid;
    default:
      Assert(false);
      break;
  }
  // should not reach here, otherwise we ignore the scan key.
  return false;
}

static bool CheckNonnullValue(const ::pax::stats::MinmaxStatistics &minmax,
                              ScanKey scan_key, Form_pg_attribute attr) {
  Oid procid;
  FmgrInfo finfo;
  Datum datum;
  Datum matches;
  auto value = scan_key->sk_argument;
  auto typid = attr->atttypid;
  auto collation = minmax.collation();
  auto typlen = attr->attlen;
  auto typbyval = attr->attbyval;

  switch (scan_key->sk_strategy) {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber: {
      auto ok = cbdb::MinMaxGetStrategyProcinfo(typid, &procid, &finfo,
                                                scan_key->sk_strategy);
      if (!ok || !CheckProcid(minmax, scan_key->sk_strategy, procid))
        return true;
      datum = pax::MicroPartitionStats::FromValue(minmax.minimal(), typlen,
                                                  typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
      matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      break;
    }
    case BTEqualStrategyNumber: {
      auto ok = cbdb::MinMaxGetStrategyProcinfo(typid, &procid, &finfo,
                                                BTLessEqualStrategyNumber);
      if (!ok || !CheckProcid(minmax, BTLessEqualStrategyNumber, procid))
        return true;
      datum = pax::MicroPartitionStats::FromValue(minmax.minimal(), typlen,
                                                  typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
      matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);

      if (!DatumGetBool(matches))
        // not (min <= value) --> min > value
        return false;

      ok = cbdb::MinMaxGetStrategyProcinfo(typid, &procid, &finfo,
                                           BTGreaterEqualStrategyNumber);
      if (!ok || !CheckProcid(minmax, BTGreaterEqualStrategyNumber, procid))
        return true;
      datum = pax::MicroPartitionStats::FromValue(minmax.maximum(), typlen,
                                                  typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
      matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      break;
    }
    case BTGreaterEqualStrategyNumber:
    case BTGreaterStrategyNumber: {
      auto ok = cbdb::MinMaxGetStrategyProcinfo(typid, &procid, &finfo,
                                                scan_key->sk_strategy);
      if (!ok || !CheckProcid(minmax, scan_key->sk_strategy, procid))
        return true;
      datum = pax::MicroPartitionStats::FromValue(minmax.maximum(), typlen,
                                                  typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
      matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      break;
    }
    default:
      Assert(false);
      matches = BoolGetDatum(true);
      break;
  }
  return DatumGetBool(matches);
}

// returns true: if the micro partition needs to scan
// returns false: the micro partition could be ignored
bool PaxFilter::TestMicroPartitionScanInternal(
    const pax::stats::MicroPartitionStatisticsInfo &stats,
    TupleDesc desc) const {
  auto natts = desc->natts;

  Assert(num_scan_keys_ > 0);
  Assert(stats.columnstats_size() <= natts);
  for (int i = 0; i < num_scan_keys_; i++) {
    auto scan_key = &scan_keys_[i];
    auto column_index = scan_key->sk_attno - 1;
    Assert(column_index >= 0 && column_index < natts);

    auto attr = &desc->attrs[column_index];
    // scan key should never contain dropped column
    Assert(!attr->attisdropped);
    // the collation in catalog and scan key should be consistent
    Assert(scan_key->sk_collation == attr->attcollation);

    if (column_index >= stats.columnstats_size())
      continue;  // missing attributes have no stats

    const auto &column_stats = stats.columnstats(column_index);
    const auto &minmax = column_stats.minmaxstats();

    // Check whether alter column type will result rewriting whole table.
    Assert(attr->atttypid == minmax.typid());

    if (scan_key->sk_flags & SK_ISNULL) {
      if (!CheckNullKey(scan_key, column_stats)) return false;
    } else if (column_stats.allnull()) {
      // ALL values are null, but the scan key is not null
      return false;
    } else if (scan_key->sk_collation != minmax.collation()) {
      // collation doesn't match ignore this scan key
    } else if (!CheckNonnullValue(minmax, scan_key, attr)) {
      return false;
    }
  }
  return true;
}

}  // namespace pax
