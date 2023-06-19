#pragma once

#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <string>

#include "comm/bitmap.h"
#include "storage/pax.h"

namespace pax {
class CPaxDeleter {
 public:
  explicit CPaxDeleter(const Relation rel, const Snapshot snapshot);

  static TM_Result DeleteTuple(const Relation relation, const ItemPointer tid,
                               const CommandId cid, const Snapshot snapshot,
                               const Snapshot crosscheck, const bool wait,
                               TM_FailureData *tmfd, const bool changingPart);

  TM_Result MarkDelete(const ItemPointer tid);

  ~CPaxDeleter();

  void ExecDelete();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> buildDeleteIterator();
  std::map<std::string, std::unique_ptr<DynamicBitmap>> block_bitmap_map_;
  const Relation rel_;
  const Snapshot snapshot_;
};  // class CPaxDeleter
}  // namespace pax
