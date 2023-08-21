#pragma once

#include <memory>
#include <string>
namespace pax {
template <typename T>
class IteratorBase {
 public:
  virtual bool HasNext() const = 0;
  virtual T Next() = 0;
  virtual void Rewind() = 0;
  virtual ~IteratorBase() = default;
};  // class IteratorBase
}  // namespace pax
