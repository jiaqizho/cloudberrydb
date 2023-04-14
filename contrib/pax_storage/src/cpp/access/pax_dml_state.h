#pragma once

#include <memory>

#include "access/pax_deleter.h"
#include "access/pax_inserter.h"
#include "comm/local_wrappers.h"
#include "comm/singleton.h"

extern "C" {
#include "postgres.h"  // NOLINT
#include "utils/hsearch.h"
}

extern "C" {
extern void pax_dml_state_reset_cb(void* _);
}

namespace pax {
struct PaxDmlState {
  Oid oid;
  CPaxInserter* inserter;
  CPaxDeleter* deleter;
};

class CPaxDmlStateLocal {
  friend class Singleton<CPaxDmlStateLocal>;

 public:
  static CPaxDmlStateLocal* instance() {
    return Singleton<CPaxDmlStateLocal>::GetInstance();
  }

  ~CPaxDmlStateLocal() {}

  void InitDmlState(const Relation rel, const CmdType operation);
  void FinishDmlState(const Relation rel, const CmdType operation);

  CPaxInserter* GetInserter(const Relation& rel);
  CPaxDeleter* GetDeleter(const Relation& rel);

  void reset() {
    last_used_state_ = nullptr;
    state_ctx_ = nullptr;
    dml_descriptor_tab_ = nullptr;
  }

 private:
  CPaxDmlStateLocal() {
    dml_descriptor_tab_ = nullptr;
    last_used_state_ = nullptr;
    state_ctx_ = nullptr;
    cb_ = {.func = pax_dml_state_reset_cb, .arg = NULL};
  }
  PaxDmlState* EntryDmlState(const Oid& oid);
  PaxDmlState* FindDmlState(const Oid& oid);
  PaxDmlState* RemoveDmlState(const Oid& oid);

  CPaxDmlStateLocal(const CPaxDmlStateLocal&) = delete;
  CPaxDmlStateLocal& operator=(const CPaxDmlStateLocal&) = delete;

  PaxDmlState* last_used_state_;
  HTAB* dml_descriptor_tab_;
  MemoryContext state_ctx_;
  MemoryContextCallback cb_;
};

}  // namespace pax
