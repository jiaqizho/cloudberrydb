#include "storage/pax_parallel.h"

#include "access/pax_visimap.h"
#include "comm/paxc_wrappers.h"
#include "comm/vec_numeric.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pg_pax_tables.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"

#include <stdexcept>
#include <algorithm>

#ifdef VEC_BUILD

namespace cbdb {
static BpChar *BpcharInput(const char *s, size_t len, int32 atttypmod) {
  CBDB_WRAP_START;
  { return bpchar_input(s, len, atttypmod); }
  CBDB_WRAP_END;
  return nullptr;
}

static VarChar *VarcharInput(const char *s, size_t len, int32 atttypmod) {
  CBDB_WRAP_START;
  { return varchar_input(s, len, atttypmod); }
  CBDB_WRAP_END;
  return nullptr;
}

static text *CstringToText(const char *s, size_t len) {
  CBDB_WRAP_START;
  { return cstring_to_text_with_len(s, len); }
  CBDB_WRAP_END;
  return nullptr;
}
}
namespace pax {

class PaxRecordBatchGenerator {
 public:
  PaxRecordBatchGenerator(const std::shared_ptr<PaxFragmentInterface> &desc): desc_(desc) {}
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() { return desc_->Next(); }

 private:
  std::shared_ptr<PaxFragmentInterface> desc_;
};

std::vector<int> SchemaToIndex(TupleDesc desc, arrow::Schema *schema) {
  std::vector<int> result;
  auto names = schema->field_names();
  for (int i = 0; i < desc->natts; i++) {
    auto attr = TupleDescAttr(desc, i);
    char *attname;

    if (attr->attisdropped) continue;

    attname = NameStr(attr->attname);
    if (std::find(names.begin(), names.end(), attname) != names.end()) {
      result.push_back(i);
    }
  }

  if (std::find(names.begin(), names.end(), "ctid") != names.end()) {
    result.push_back(SelfItemPointerAttributeNumber);
  }
  if (result.size() == names.size()) return result;
  throw std::logic_error("unknown name in schema");
}

template <typename T>
class ParallelIteratorImpl : public ParallelIterator<T> {
 public:
  ParallelIteratorImpl(std::vector<T> &&v): v_(std::move(v)), index_(0) {}
  virtual ~ParallelIteratorImpl() = default;
  std::optional<T> Next() override;
 private:
  std::vector<T> v_;
  std::atomic_int32_t index_;
};

template <typename T>
std::optional<T> ParallelIteratorImpl<T>::Next() {
  int32_t index;
  int32_t next;
  auto size = static_cast<int32_t>(v_.size());

  index = index_.load(std::memory_order_relaxed);
  do {
    next = index + 1;
  } while (index < size &&
          !index_.compare_exchange_weak(index, next, std::memory_order_relaxed));

  if (index >= size)
    return std::optional<T>();
  return std::optional<T>(v_[index]);
}

PaxFragmentInterface::PaxFragmentInterface(const std::shared_ptr<ParallelScanDesc> &scan_desc)
  : scan_desc_(scan_desc)
  , block_no_(-1) {
  Assert(scan_desc && scan_desc->GetRelation());

  relation_ = scan_desc->GetRelation();
}

void PaxFragmentInterface::Release() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

void PaxFragmentInterface::InitAdapter() {
  if (adapter_)
    adapter_->Reset();
  else
    adapter_ = std::make_shared<VecAdapter>(RelationGetDescr(relation_), 0, scan_desc_->ShouldBuildCtid());
}

// return true if open micro partition successfully, false if no more micro partition left
bool PaxFragmentInterface::OpenFile() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
    visimap_ = nullptr;
  }

  auto op = scan_desc_->Iterator()->Next();
  if (!op) return false;

  auto file_system = Singleton<LocalFileSystem>::GetInstance();
  auto m = std::move(op).value();
  auto filter = scan_desc_->GetPaxFilter();

  MicroPartitionReader::ReaderOptions options;
  block_no_ = m.GetMicroPartitionId();

  // TODO: convert arrow filter to pax filter
  options.filter = filter;
  options.reused_buffer = nullptr;
  {
    const auto &visimap_name = m.GetVisibilityBitmapFile();
    if (!visimap_name.empty()) {
      visimap_ = pax::LoadVisimap(file_system, nullptr, visimap_name);
      BitmapRaw<uint8_t> raw(visimap_->data(), visimap_->size());
      options.visibility_bitmap = std::make_shared<Bitmap8>(raw, BitmapTpl<uint8>::ReadOnlyRefBitmap);
    }
  }

  InitAdapter();

  auto reader = std::make_unique<OrcReader>(file_system->Open(
      m.GetFileName(), fs::kReadMode));

  reader_ = std::make_unique<PaxVecReader>(std::move(reader), adapter_, filter);
  reader_->Open(options);
  return true;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> PaxFragmentInterface::Next() {
  std::shared_ptr<arrow::RecordBatch> result;

  if (reader_) result = reader_->ReadBatch(this);

  while (!result) {
    // If no more tuples in the current reader, try next micro partition
    if (!OpenFile())
      // No micro partitions left
      return arrow::IterationTraits<std::shared_ptr<arrow::RecordBatch>>::End();

    // If the current micro partition has no tuple, try next one
    result = reader_->ReadBatch(this);
  }
  return result;
}

static std::pair<const char *, size_t> extract_field_name(const std::string &name) {
  const char *p = name.c_str();
  auto idx = name.find_last_of('(');

  if (idx == std::string::npos)
    return {p, name.size()};

  return {p, idx};
}

static int find_field_index(const std::vector<std::pair<const char *, size_t>> &table_names, const std::pair<const char *, size_t> &kname) {
  auto num = table_names.size();

  for (size_t i = 0; i < num; i++) {
    const auto &fname = table_names[i];
    if (fname.second == kname.second && memcmp(fname.first, kname.first, fname.second) == 0)
      return i;
  }

  if (kname.second == 4 && memcmp(kname.first, "ctid", 4) == 0)
    return SelfItemPointerAttributeNumber;

  throw std::string("Not found field name:") + kname.first;
}

arrow::Result<arrow::RecordBatchIterator> PaxFragmentInterface::ScanBatchesAsyncImpl(const std::shared_ptr<arrow::dataset::ScanOptions>& options) {
  PaxRecordBatchGenerator g(std::dynamic_pointer_cast<PaxFragmentInterface>(shared_from_this()));

  Assert(scan_desc_->ScanSchema()->Equals(options->projected_schema));
  //Assert(!options->projection.IsBound());

  return arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>(std::move(g));
}

void ParallelScanDesc::CalculateScanColumns(const std::vector<std::pair<const char *, size_t>> &table_names) {
  Assert(!pax_filter_);
  Assert(scan_columns_.empty());

  auto natts = RelationGetNumberOfAttributes(relation_);
  std::vector<bool> proj_bits(natts, false);

  build_ctid_bitmap_ = false;
  for (int i = 0, n = scan_schema_->num_fields(); i < n; i++) {
    const auto &name = scan_schema_->field(i)->name();
    auto pname = extract_field_name(name);

    auto index = find_field_index(table_names, pname);

    if (index >= 0) {
      Assert(!proj_bits[index]); // only once
      scan_columns_.push_back(index);
      proj_bits[index] = true;
    } else {
      Assert(index == SelfItemPointerAttributeNumber);
      Assert(!build_ctid_bitmap_); // only once

      build_ctid_bitmap_ = true;
      scan_columns_.push_back(SelfItemPointerAttributeNumber);
    }
  }

  pax_filter_ = std::make_shared<PaxFilter>();
  pax_filter_->SetColumnProjection(std::move(proj_bits));
}

template <typename T>
inline auto ToScalarValue(const arrow::Scalar *scalar) {
  auto derived = dynamic_cast<const T*>(scalar);
  Assert(derived);
  return derived->value;
}

static std::pair<Datum, bool> ArrowScalarToDatum(const std::shared_ptr<arrow::Scalar> &p_scalar,
                                                 Form_pg_attribute attr) {
  auto scalar = p_scalar.get();

  switch (scalar->type->id()) {
    case arrow::Type::BOOL:
    {
      auto v = ToScalarValue<arrow::BooleanScalar>(scalar);
      return {BoolGetDatum(v), true};
    }
    case arrow::Type::INT8:
    {
      auto v = ToScalarValue<arrow::Int8Scalar>(scalar);
      return {Int8GetDatum(v), true};
    }
    case arrow::Type::INT16:
    {
      auto v = ToScalarValue<arrow::Int16Scalar>(scalar);
      return {Int16GetDatum(v), true};
    }
    case arrow::Type::INT32:
    {
      // candicate: int4, xid, oid
      auto v = ToScalarValue<arrow::Int32Scalar>(scalar);
      return {Int32GetDatum(v), true};
    }
    case arrow::Type::INT64:
    {
      auto v = ToScalarValue<arrow::Int64Scalar>(scalar);
      return {Int64GetDatum(v), true};
    }
    case arrow::Type::FLOAT:
    {
      auto v = ToScalarValue<arrow::FloatScalar>(scalar);
      return {Float4GetDatum(v), true};
    }
    case arrow::Type::DOUBLE:
    {
      auto v = ToScalarValue<arrow::DoubleScalar>(scalar);
      return {Float8GetDatum(v), true};
    }
    case arrow::Type::DATE32:
    {
      auto v = ToScalarValue<arrow::Date32Scalar>(scalar);
      return {Int32GetDatum(v), true};
    }
    case arrow::Type::NUMERIC128:
    {
      auto const v = ToScalarValue<arrow::Numeric128Scalar>(scalar);
      auto high = static_cast<int64>(v.high_bits());
      auto low = static_cast<int64>(v.low_bits());
      auto datum = vec_short_numeric_to_datum(&low, &high);
      return {datum, true};
    }
    case arrow::Type::TIMESTAMP:
    {
      auto v = ToScalarValue<arrow::TimestampScalar>(scalar);
      switch (attr->atttypid)
      {
      case TIMEOID:
      case TIMESTAMPOID:
      case TIMESTAMPTZOID:
        return {Int64GetDatum(v), true};
      default:
        return {0, false};
      }
      break;
    }
    case arrow::Type::STRING:
    {
      auto v = ToScalarValue<arrow::StringScalar>(scalar);
      const char *s = reinterpret_cast<const char *>(v->data());
      auto len = static_cast<size_t>(v->size());
      switch (attr->atttypid) {
        case TEXTOID:
          return {PointerGetDatum(cbdb::CstringToText(s, len)), true};
        case VARCHAROID:
          // trailing spaces will be removed
          return {PointerGetDatum(cbdb::VarcharInput(s, len, attr->atttypmod)), true};
        case BPCHAROID:
          // stripped spaces will be added
          return {PointerGetDatum(cbdb::BpcharInput(s, len, attr->atttypmod)), true};
        default: break;
      }
      return {0, false};
    }
    default: break;
  }
  return {0, false};
}

static bool InitializeScanKey(ScanKey key, int flags, AttrNumber attno, StrategyNumber strategy, Oid subtype, Oid collation, Datum argument) {
  key->sk_flags = flags;
  key->sk_attno = attno;
  key->sk_strategy = strategy;
  key->sk_subtype = subtype;
  key->sk_collation = collation;
  key->sk_argument = argument;

  MemSet(&key->sk_func, 0, sizeof(key->sk_func));
  return true;
}

static inline Oid adjustRValueType(Oid ltype, Oid candicate_rtype) {
  switch (ltype) {
    case XIDOID:
    case OIDOID:
    case CIDOID:
    case REGPROCOID:
      if (candicate_rtype == INT4OID)
        return ltype;
  }
  return candicate_rtype;
}

static bool InitializeScanKey(ScanKey key, Form_pg_attribute attr, StrategyNumber strategy, const arrow::Datum *argument) {
  auto value = ArrowScalarToDatum(argument->scalar(), attr);
  if (value.second) {
    auto oid = attr->atttypid;
    return InitializeScanKey(key, 0, attr->attnum, strategy, oid, attr->attcollation, value.first);
  }
  return false;
}

void ParallelScanDesc::TransformFilterExpression(const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options, const arrow::compute::Expression &expr, const std::vector<std::pair<const char *, size_t>> &table_names) {
  ScanKeyData key;
  StrategyNumber strategy;
  bool invert_flag = false;
  auto call = expr.call();

  // top level must be call
  if (!call) return;
  if (call->function_name == "and_kleene") {
    for (const auto &e : call->arguments) {
      TransformFilterExpression(scan_options, e, table_names);
    }
    return;
  }
  if (call->function_name == "invert") {
    Assert(call->arguments.size() == 1);
    // only support invert -> is_null
    if (!(call = call->arguments[0].call())) return;

    invert_flag = true;
  }

  auto nargs = call->arguments.size();
  // unexpected expressions
  Assert(nargs > 0);
  const auto &ex1 = call->arguments[0];
  const auto fr = ex1.field_ref();
  if (!fr || !fr->IsName()) return;

  // 1. get attnum(index + 1), typid, etc. of field ref
  auto pname = extract_field_name(*fr->name());
  auto index = find_field_index(table_names, pname);
  if (index < 0) return; // not found name

  auto tupdesc = RelationGetDescr(relation_);
  Assert(index < tupdesc->natts);
  auto attr = TupleDescAttr(tupdesc, index);

  if (call->function_name == "is_null") {
    Assert(call->arguments.size() == 1);

    int flags = SK_ISNULL;
    flags |= invert_flag ? SK_SEARCHNOTNULL : SK_SEARCHNULL;

    ScanKeyData k{.sk_flags = flags, .sk_attno = (AttrNumber)(index + 1), };
    filter_scan_keys_.emplace_back(k);
    return;
  }

  // assume invert only support `is_null`
  if (nargs != 2 || invert_flag) return;

  const auto &ex2 = call->arguments[1];
  const auto v = ex2.literal();

  // call should be like a > 2
  if (!v || !v->is_scalar()) return;

  // 2. convert arrow datum to pg datum, subtype
  if (call->function_name == "greater") {
    strategy = BTGreaterStrategyNumber;
  } else if (call->function_name == "greater_equal") {
    strategy = BTGreaterEqualStrategyNumber;
  } else if (call->function_name == "less") {
    strategy = BTLessStrategyNumber;
  } else if (call->function_name == "less_equal") {
    strategy = BTLessEqualStrategyNumber;
  } else if (call->function_name == "equal") {
    strategy = BTEqualStrategyNumber;
  } else {
    // not support operator
    return;
  }

  if (!InitializeScanKey(&key, attr, strategy, v)) return;

  filter_scan_keys_.emplace_back(key);
}

arrow::Status ParallelScanDesc::Initialize(uint32_t tableoid, const std::shared_ptr<arrow::Schema> &table_schema, const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options) {
  relation_ = cbdb::TableOpen(tableoid, AccessShareLock);
  auto tupdesc = RelationGetDescr(relation_);

  std::vector<MicroPartitionMetadata> result;

  table_schema_ = table_schema;
  scan_schema_ = scan_options->dataset_schema;

  std::vector<std::pair<const char *, size_t>> table_names;
  for (int i = 0, n = table_schema_->num_fields(); i < n; i++)
    table_names.push_back(extract_field_name(table_schema_->field(i)->name()));

  CalculateScanColumns(table_names);
  TransformFilterExpression(scan_options, scan_options->filter, table_names);
  Assert(pax_filter_);
  if (!filter_scan_keys_.empty())
    pax_filter_->SetScanKeys(filter_scan_keys_.data(), filter_scan_keys_.size());

  auto it = MicroPartitionInfoIterator::New(relation_, nullptr);
  while (it->HasNext()) {
    auto meta = it->Next();
    bool ok = true;
    if (!filter_scan_keys_.empty()) {
      MicroPartitionStatsProvider provider(meta.GetStats());
      ok = pax_filter_->TestScan(provider, tupdesc, PaxFilterStatisticsKind::kFile);
    }
    if (ok)
      result.emplace_back(std::move(meta));
  }
  it->Release();

  num_micro_partitions_ = static_cast<int>(result.size());
  iterator_ = std::make_unique<ParallelIteratorImpl<MicroPartitionMetadata>>(std::move(result));

  return arrow::Status::OK();
}

void ParallelScanDesc::Release() {
  Assert(relation_);

  if (pax_enable_debug && pax_filter_) {
    pax_filter_->LogStatistics();
  }
  cbdb::TableClose(relation_, NoLock);
  relation_ = nullptr;
  iterator_ = nullptr;
}

ParallelScanDesc::FragmentIteratorInternal::FragmentIteratorInternal(const std::shared_ptr<ParallelScanDesc> &desc)
 : desc_(desc), fragment_counter_(0) {
}

arrow::Result<std::shared_ptr<arrow::dataset::Fragment>> ParallelScanDesc::FragmentIteratorInternal::Next() {
  // limit the number of fragment threads.
  if (fragment_counter_ >= desc_->num_micro_partitions_)
    return arrow::IterationTraits<std::shared_ptr<arrow::dataset::Fragment>>::End();

  fragment_counter_++;
  return std::static_pointer_cast<arrow::dataset::Fragment>(std::make_shared<PaxFragmentInterface>(desc_));
}

arrow::Result<arrow::dataset::FragmentIterator> PaxDatasetInterface::GetFragmentsImpl(arrow::compute::Expression predicate) {
  ParallelScanDesc::FragmentIteratorInternal it(desc_);
  return arrow::Iterator<std::shared_ptr<arrow::dataset::Fragment>>(std::move(it));
}

} // namespace pax

#endif