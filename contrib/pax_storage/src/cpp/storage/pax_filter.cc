#include "storage/pax_filter.h"

namespace pax {

PaxFilter::PaxFilter() : proj_(nullptr) {}

PaxFilter::~PaxFilter() {
  if (proj_) delete proj_;
}

bool *PaxFilter::GetColumnProjection() { return proj_; }

void PaxFilter::SetColumnProjection(bool *proj) { proj_ = proj; }

}  // namespace pax
