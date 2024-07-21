#pragma once

#include "comm/cbdb_api.h"

namespace pax {
namespace clustering {
class Sorter {
 public:
  enum TupleSorterType {
    kTupleSorterTypeHeap,
    kTupleSorterTypeIndex,
  };
  struct TupleSorterOptions {
    TupleSorterType type;
  };

 public:
  Sorter() = default;
  virtual ~Sorter() = default;
  virtual void AppendSortData(TupleTableSlot *slot) = 0;
  virtual void Sort() = 0;
  virtual bool GetSortedData(TupleTableSlot *slot) = 0;
};
}  // namespace clustering

}  // namespace pax
