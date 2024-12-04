#include "storage/micro_partition_file_factory.h"

#include "comm/pax_memory.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_row_filter_reader.h"
#include "storage/orc/porc.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace pax {
std::unique_ptr<MicroPartitionReader>
MicroPartitionFileFactory::CreateMicroPartitionReader(
    const MicroPartitionReader::ReaderOptions &options, int32 flags,
    std::shared_ptr<File> file, std::shared_ptr<File> toast_file) {
  std::unique_ptr<MicroPartitionReader> reader =
      std::make_unique<OrcReader>(file, toast_file);

#ifdef VEC_BUILD
  if (flags & ReaderFlags::FLAGS_VECTOR_PATH) {
#if 0
    auto max_batch_size = VecAdapter::GetMaxBatchSizeFromStr(
        cbdb::GetGUCConfigOptionByName(VECTOR_MAX_BATCH_SIZE_GUC_NAME, NULL,
                                       true),
        VEC_BATCH_LENGTH);

    // The max of record batch size must align with 8
    // Because the begin bits of the null bitmap in pax must be aligned 8
    CBDB_CHECK(
        max_batch_size > 0 && (max_batch_size % MEMORY_ALIGN_SIZE == 0),
        cbdb::CException::kExTypeInvalid,
        fmt("Invalid GUC setting [%s=%d], it should align with %d",
            VECTOR_MAX_BATCH_SIZE_GUC_NAME, max_batch_size, MEMORY_ALIGN_SIZE));
#endif

    auto vec_adapter_ptr = std::make_shared<VecAdapter>(
        options.tuple_desc, 0,
        (flags & ReaderFlags::FLAGS_SCAN_WITH_CTID) != 0);
    reader = std::make_unique<PaxVecReader>(std::move(reader), vec_adapter_ptr,
                                            options.filter);
  } else
#endif
      if (options.filter && options.filter->GetRowFilter()) {
    reader = MicroPartitionRowFilterReader::New(
        std::move(reader), options.filter, options.visibility_bitmap);
  }

  reader->Open(options);
  return reader;
}

std::unique_ptr<MicroPartitionWriter>
MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const MicroPartitionWriter::WriterOptions &options,
    std::shared_ptr<File> file, std::shared_ptr<File> toast_file) {
  std::vector<pax::porc::proto::Type_Kind> type_kinds;
  type_kinds = OrcWriter::BuildSchema(
      options.rel_tuple_desc,
      options.storage_format == PaxStorageFormat::kTypeStoragePorcVec);
  return std::make_unique<OrcWriter>(options, std::move(type_kinds), file,
                                     toast_file);
}

}  // namespace pax
