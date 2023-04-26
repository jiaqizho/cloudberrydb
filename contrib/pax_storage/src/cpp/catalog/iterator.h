#pragma once
#include <memory>
#include <string>
namespace pax {
typedef enum IteratorSeekPosType {
  ITER_SEEK_POS_BEGIN,
  ITER_SEEK_POS_CUR,
  ITER_SEEK_POS_END
} IteratorSeekPosType;

template <typename T>
class IteratorBase {
 public:
  virtual void Init() = 0;
  virtual bool HasNext() = 0;
  virtual std::shared_ptr<T>& Next() = 0;
  virtual void Seek(int offset, IteratorSeekPosType whence) = 0;
  virtual ~IteratorBase() {}
};  // class IteratorBase
}  // namespace pax
