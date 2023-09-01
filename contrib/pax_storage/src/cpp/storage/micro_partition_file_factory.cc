#include "storage/micro_partition_file_factory.h"

#include "storage/orc/orc.h"

namespace pax {
MicroPartitionReader *MicroPartitionFileFactory::CreateMicroPartitionReader(
    const std::string &type, File *file,
    const MicroPartitionReader::ReaderOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    MicroPartitionReader *reader = new OrcReader(file);

    reader->Open(options);
    return reader;
  }

  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

MicroPartitionWriter *MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const std::string &type, File *file,
    const MicroPartitionWriter::WriterOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    std::vector<orc::proto::Type_Kind> type_kinds;
    std::vector<ColumnEncoding_Kind> encoding_types;
    MicroPartitionWriter *writer = nullptr;
    std::tie(type_kinds, encoding_types) = OrcWriter::BuildSchema(options);
    writer = new OrcWriter(std::move(options), std::move(type_kinds),
                           std::move(encoding_types), file);
    return writer;
  }
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

}  // namespace pax
