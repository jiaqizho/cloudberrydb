#pragma once

#include <memory>
#include <string>
namespace pax {
typedef enum IteratorSeekPosType {
  BEGIN = 0,
  CURRENT = 1,
  END = 2
} IteratorSeekPosType;
template <typename T>
class IteratorBase {
 public:
  virtual void Init() = 0;
  virtual bool Empty() const = 0;
  virtual uint32_t Size() const = 0;
  virtual bool HasNext() const = 0;
  virtual std::shared_ptr<T>& Next() = 0;
  virtual std::shared_ptr<T>& Current() const = 0;
  virtual void Seek(int offset, IteratorSeekPosType whence) = 0;
  virtual ~IteratorBase() {}
};  // class IteratorBase
}  // namespace pax
