#include "access/pax_dml_state.h"

#define PAX_DEFAULT_GP_INTERCONNECT_QUEUE_DEPTH 64
#define PAX_DEFAULT_COLUMN_NUM_OF_WIDE_TABLE 64

namespace pax {
// class CPaxDmlStateLocal

void CPaxDmlStateLocal::DmlStateResetCallback(void * /*arg*/) {
  pax::CPaxDmlStateLocal::Instance()->Reset();
}

void CPaxDmlStateLocal::InitDmlState(Relation rel, CmdType operation) {
  if (Gp_role == GP_ROLE_DISPATCH &&
      Gp_interconnect_queue_depth < PAX_DEFAULT_GP_INTERCONNECT_QUEUE_DEPTH &&
      rel->rd_att->natts > PAX_DEFAULT_COLUMN_NUM_OF_WIDE_TABLE) {
    elog(WARNING,
         "in pax table am, we recommend that gp_interconnect_queue_depth is a "
         "value greater than or equal to 64, but current value is %d",
         Gp_interconnect_queue_depth);
  }

  if (!cbdb::pax_memory_context) {
    cbdb::pax_memory_context = AllocSetContextCreate(
        CurrentMemoryContext, "Pax Storage", PAX_ALLOCSET_DEFAULT_SIZES);

    cbdb::MemoryCtxRegisterResetCallback(cbdb::pax_memory_context, &cb_);
  }

  auto oid = cbdb::RelationGetRelationId(rel);
  last_state_ = std::make_shared<DmlStateValue>();
  last_oid_ = oid;
  dml_descriptor_tab_[oid] = last_state_;
}

void CPaxDmlStateLocal::FinishDmlState(Relation rel, CmdType /*operation*/) {
  auto state = RemoveDmlState(cbdb::RelationGetRelationId(rel));

  if (state == nullptr) return;

  if (state->deleter) {
    state->deleter->ExecDelete();

    state->deleter = nullptr;
  }

  if (state->inserter) {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    state->inserter->FinishInsert();
    MemoryContextSwitchTo(old_ctx);
    state->inserter = nullptr;
  }
}

CPaxInserter *CPaxDmlStateLocal::GetInserter(Relation rel) {
  auto state = FindDmlState(cbdb::RelationGetRelationId(rel));
  if (state->inserter == nullptr) {
    state->inserter = std::make_unique<CPaxInserter>(rel);
  }
  return state->inserter.get();
}

CPaxDeleter *CPaxDmlStateLocal::GetDeleter(Relation rel, Snapshot snapshot,
                                           bool missing_null) {
  auto state = FindDmlState(cbdb::RelationGetRelationId(rel));
  if (state->deleter == nullptr && !missing_null) {
    state->deleter = std::make_unique<CPaxDeleter>(rel, snapshot);
  }
  return state->deleter.get();
}

void CPaxDmlStateLocal::Reset() { cbdb::pax_memory_context = nullptr; }

CPaxDmlStateLocal::CPaxDmlStateLocal()
    : last_oid_(InvalidOid), cb_{.func = DmlStateResetCallback, .arg = NULL} {}

std::shared_ptr<CPaxDmlStateLocal::DmlStateValue>
CPaxDmlStateLocal::RemoveDmlState(const Oid &oid) {
  std::shared_ptr<CPaxDmlStateLocal::DmlStateValue> value;

  auto it = dml_descriptor_tab_.find(oid);
  if (it != dml_descriptor_tab_.end()) {
    value = it->second;
    dml_descriptor_tab_.erase(it);

    if (last_oid_ == oid) {
      last_oid_ = InvalidOid;
      last_state_ = nullptr;
    }
  }
  return value;
}

std::shared_ptr<CPaxDmlStateLocal::DmlStateValue>
CPaxDmlStateLocal::FindDmlState(const Oid &oid) {
  Assert(OidIsValid(oid));

  if (this->last_oid_ == oid) return last_state_;

  auto it = dml_descriptor_tab_.find(oid);
  if (it != dml_descriptor_tab_.end()) {
    last_oid_ = oid;
    last_state_ = it->second;
    return last_state_;
  }

  return nullptr;
}

}  // namespace pax
