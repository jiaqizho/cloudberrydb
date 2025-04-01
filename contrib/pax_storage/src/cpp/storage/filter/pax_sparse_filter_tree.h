#pragma once
#include "comm/cbdb_api.h"

#include <string>
#include <vector>

#include "comm/fmt.h"
namespace pax {

enum PFTNodeType {
  InvalidType = 0,
  OpType,
  ConstType,
  VarType,
  NullTestType,
  InType,
  AndType,
  OrType,
  NotType,
  ResultType,  // only used in `exec*`
  UnsupportedType,
};

struct PFTNode {  // PTF = PAX filter tree
  PFTNodeType type;
  std::shared_ptr<PFTNode> parent;  // parent node
  int subidx;                       // the position in parent node
  std::vector<std::shared_ptr<PFTNode>> sub_nodes;

  PFTNode() : type(InvalidType), parent(nullptr), subidx(-1) {}
  PFTNode(PFTNodeType t) : type(t), parent(nullptr), subidx(-1) {}

  static inline void AppendSubNode(const std::shared_ptr<PFTNode> &p_node,
                                   std::shared_ptr<PFTNode> &&sub_node) {
    sub_node->parent = p_node;
    sub_node->subidx = p_node->sub_nodes.size();
    p_node->sub_nodes.emplace_back(std::move(sub_node));
  }

  virtual ~PFTNode() = default;

  virtual std::string DebugString() const { return "Empty Node(Invalid)"; }
};

struct OpNode final : public PFTNode {
  Oid opno = InvalidOid;  // optional fields, can be InvalidOid
  StrategyNumber strategy = InvalidStrategy;
  Oid collation = InvalidOid;
  Oid left_typid = InvalidOid;
  Oid right_typid = InvalidOid;

  OpNode() : PFTNode(OpType) {}

  std::string DebugString() const override {
    return ::pax::fmt(
        "OpNode(opno=%d, strategy=%d, collation=%d, ltypid=%d, rtypid=%d)",
        opno, strategy, collation, left_typid, right_typid);
  }
};

struct ConstNode final : public PFTNode {
  Datum const_val = 0;
  int sk_flags = 0;
  // only used in opexpr(const, const);
  // other case, we will compare the typid in Var and const typid
  Oid const_type = InvalidOid;

  ConstNode() : PFTNode(ConstType), const_val(0) {}

  std::string DebugString() const override {
    return ::pax::fmt("ConstNode(const_type=%d, flags=%d)", const_type,
                      sk_flags);
  }
};

struct VarNode final : public PFTNode {
  AttrNumber attrno = InvalidAttrNumber;

  VarNode() : PFTNode(VarType) {}

  std::string DebugString() const override {
    return ::pax::fmt("VarNode(attrno=%d)", attrno);
  }
};

struct InNode final : public PFTNode {
  bool in = false;
  InNode() : PFTNode(InType) {}

  std::string DebugString() const override {
    return ::pax::fmt("InNode(in=%s)", BOOL_TOSTRING(in));
  }
};

struct AndNode final : public PFTNode {
  AndNode() : PFTNode(AndType) {}

  std::string DebugString() const override { return "AndNode"; }
};

struct OrNode final : public PFTNode {
  OrNode() : PFTNode(OrType) {}

  std::string DebugString() const override { return "OrNode"; }
};

struct NotNode final : public PFTNode {
  int sk_flags = 0;
  NotNode() : PFTNode(NotType) {}

  std::string DebugString() const override { return "NotNode"; }
};

struct NullTestNode final : public PFTNode {
  int sk_flags = 0;
  NullTestNode() : PFTNode(NullTestType) {}

  std::string DebugString() const override {
    return ::pax::fmt("NullTestNode(flags=%d)", sk_flags);
  }
};

struct UnsupportedNode final : public PFTNode {
  UnsupportedNode(std::string reason)
      : PFTNode(UnsupportedType), reason_(std::move(reason)) {}

  std::string DebugString() const override {
    return ::pax::fmt("UnsupportedNode(reason=%s)", reason_.c_str());
  }

 private:
  std::string reason_;
};

}  // namespace pax