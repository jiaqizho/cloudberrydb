#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/pax_defined.h"

namespace pax {

struct ExecutionFilterContext {
  ExprContext *econtext;
  ExprState *estate_final = nullptr;
  ExprState **estates;
  AttrNumber *attnos;
  int size = 0;
  inline bool HasExecutionFilter() const { return size > 0 || estate_final; }
};

class PaxRowFilter final {
public:
  PaxRowFilter();

  bool Initialize(Relation rel, PlanState *ps,
                const std::vector<bool> &projection);

  inline const ExecutionFilterContext *GetExecutionFilterContext() const {
    return &efctx_;
  }

  inline const std::vector<AttrNumber> &GetRemainingColumns() const {
    return remaining_attnos_;
  }
  
private:
  void FillRemainingColumns(Relation rel, const std::vector<bool> &projection);

private:
  ExecutionFilterContext efctx_;
  // all selected columns - single row filting columns
  // before running final cross columns expression filtering, the remaining
  // columns should be filled.
  std::vector<AttrNumber> remaining_attnos_;
};


}; 