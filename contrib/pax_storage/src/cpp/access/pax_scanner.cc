#include "access/pax_scanner.h"

#include "catalog/table_metadata.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc_native_micro_partition.h"
#include "storage/pax.h"
extern "C" {
#include "postgres.h"  // NOLINT
#include "utils/snapshot.h"
}

namespace pax {

PaxScanDesc* CPaxScannner::CreateTableScanDesc(
    const Relation relation, const Snapshot snapshot, const int nkeys,
    const struct ScanKeyData* key, const ParallelTableScanDesc pscan,
    const uint32 flags) {
  PaxScanDesc* desc = new PaxScanDesc();
  desc->rs_base.rs_rd = relation;
  desc->rs_base.rs_snapshot = snapshot;
  desc->rs_base.rs_nkeys = nkeys;
  desc->rs_base.rs_flags = flags;
  desc->rs_base.rs_parallel = pscan;

  // TODO(gongxun): use custom memory context
  desc->scanner = new CPaxScannner(&desc->rs_base, key);
  return desc;
}

void CPaxScannner::ScanTableReScan(PaxScanDesc* desc) {
  Assert(desc->scanner->reader_ != nullptr);
  desc->scanner->reader_->ReOpen();
}

CPaxScannner::CPaxScannner(const TableScanDesc scan_desc,
                           const ScanKeyData* key)
    : scan_desc_(scan_desc), key_(key) {
  TableMetadata* meta_info;
  meta_info = TableMetadata::Create(scan_desc->rs_rd, scan_desc->rs_snapshot);

  FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReaderPtr micro_partition_reader =
      new OrcNativeMicroPartitionReader(file_system);

  reader_ = new TableReader(micro_partition_reader, meta_info->NewIterator());

  reader_->Open();
}

bool CPaxScannner::GetNextSlot(const ScanDirection direction,
                               TupleTableSlot* slot) {
  CTupleSlot cslot(slot);
  return reader_->ReadTuple(&cslot);
}

}  // namespace pax
