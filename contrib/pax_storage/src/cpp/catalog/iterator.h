#pragma once
#include <memory>
#include <string>
namespace pax {
template <typename T>
class IteratorBase {
 public:
  virtual void Init() = 0;
  virtual bool HasNext() = 0;
  virtual std::shared_ptr<T>& Next() = 0;
  virtual ~IteratorBase() {}
};  // class IteratorBase
}  // namespace pax
