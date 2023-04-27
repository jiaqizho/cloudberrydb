#include "access/pax_dml_state.h"

#include "access/pax_deleter.h"
#include "access/pax_inserter.h"

namespace pax {
// class CPaxDmlStateLocal

void CPaxDmlStateLocal::DmlStateResetCallback(void *_) {
  pax::CPaxDmlStateLocal::instance()->reset();
}

void CPaxDmlStateLocal::InitDmlState(const Relation rel,
                                     const CmdType operation) {
  if (!this->dml_descriptor_tab_) {
    HASHCTL hash_ctl;
    Assert(this->state_ctx_ == NULL);
    this->state_ctx_ = cbdb::AllocSetCtxCreate(
        CurrentMemoryContext, "Pax DML State Context", ALLOCSET_SMALL_SIZES);
    cbdb::MemoryCtxRegisterResetCallback(state_ctx_, &this->cb_);

    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PaxDmlState);
    hash_ctl.hcxt = this->state_ctx_;
    this->dml_descriptor_tab_ = cbdb::HashCreate(
        "Pax DML state", 128, &hash_ctl, HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
  }

  this->EntryDmlState(cbdb::RelationGetRelationId(rel));
}

void CPaxDmlStateLocal::FinishDmlState(const Relation rel,
                                       const CmdType operation) {
  PaxDmlState *state;
  state = RemoveDmlState(cbdb::RelationGetRelationId(rel));

  if (!state) return;

  if (state->deleter) {
    // TODO(gongxun): deleter finish
    delete state->deleter;
    state->deleter = nullptr;
    // FIXME: it's update operation, maybe we should do something here
  }

  if (state->inserter) {
    // TODO(gongxun): inserter finish
    state->inserter->FinishInsert();
    delete state->inserter;
    state->inserter = nullptr;
  }
}

CPaxInserter *CPaxDmlStateLocal::GetInserter(const Relation &rel) {
  PaxDmlState *state;
  state = FindDmlState(cbdb::RelationGetRelationId(rel));
  // TODO(gongxun): switch memory context??
  if (state->inserter == nullptr) {
    state->inserter = new CPaxInserter(rel);
  }
  return state->inserter;
}

CPaxDeleter *CPaxDmlStateLocal::GetDeleter(const Relation &rel) {
  PaxDmlState *state;
  state = FindDmlState(cbdb::RelationGetRelationId(rel));
  // TODO(gongxun): switch memory context??
  if (state->deleter == nullptr) {
    state->deleter = new CPaxDeleter(rel);
  }
  return state->deleter;
}

PaxDmlState *CPaxDmlStateLocal::EntryDmlState(const Oid &oid) {
  PaxDmlState *state;
  bool found;
  Assert(this->dml_descriptor_tab_);

  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_ENTER, &found));
  state->inserter = nullptr;
  state->deleter = nullptr;
  Assert(!found);

  this->last_used_state_ = state;
  return state;
}
PaxDmlState *CPaxDmlStateLocal::RemoveDmlState(const Oid &oid) {
  Assert(this->dml_descriptor_tab_);

  PaxDmlState *state;
  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_REMOVE, NULL));

  if (!state) return NULL;

  if (this->last_used_state_ && this->last_used_state_->oid == oid)
    this->last_used_state_ = NULL;

  return state;
}

PaxDmlState *CPaxDmlStateLocal::FindDmlState(const Oid &oid) {
  Assert(this->dml_descriptor_tab_);

  if (this->last_used_state_ && this->last_used_state_->oid == oid)
    return last_used_state_;

  PaxDmlState *state;
  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_FIND, NULL));
  Assert(state);

  this->last_used_state_ = state;
  return state;
}

}  // namespace pax
