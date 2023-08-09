#pragma once

namespace pax {

class PaxFilter final {
 public:
  PaxFilter();

  ~PaxFilter();

  bool *GetColumnProjection();

  void SetColumnProjection(bool *proj);

 private:
  bool *proj_;
};  // class PaxFilter

}  // namespace pax
