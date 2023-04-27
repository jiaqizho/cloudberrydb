#pragma once

#include "comm/cbdb_api.h"

#include "storage/pax.h"

namespace pax {
struct PaxScanDesc;

class CPaxScannner {
 public:
  static PaxScanDesc* CreateTableScanDesc(const Relation relation,
                                          const Snapshot snapshot,
                                          const int nkeys,
                                          const struct ScanKeyData* key,
                                          const ParallelTableScanDesc pscan,
                                          const uint32 flags);

  ~CPaxScannner() {}

  void ScanTableReScan(PaxScanDesc* desc);

  bool GetNextSlot(const ScanDirection direction, TupleTableSlot* slot);

 private:
  CPaxScannner(const TableScanDesc scan_desc, const ScanKeyData* key);
  CPaxScannner() = delete;

  TableReader* reader_;
  TableScanDesc scan_desc_;
  const ScanKeyData* key_;
};  // class CPaxScanner

struct PaxScanDesc {
  TableScanDescData rs_base;
  CPaxScannner* scanner;
};

}  // namespace pax
