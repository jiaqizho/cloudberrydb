#include "storage/micro_partition_file_factory.h"

#include "storage/orc/orc.h"

namespace pax {
MicroPartitionReader *MicroPartitionFileFactory::CreateMicroPartitionReader(
    const std::string &type, File *file,
    const MicroPartitionReader::ReaderOptions & /*options*/) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    MicroPartitionReader *reader = nullptr;
    reader = OrcReader::CreateReader(file);
    return reader;
  }

  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

MicroPartitionWriter *MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const std::string &type, File *file,
    const MicroPartitionWriter::WriterOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    MicroPartitionWriter *writer = nullptr;
    writer = new OrcWriter(options, OrcWriter::BuildSchema(options.desc), file);
    return writer;
  }
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

}  // namespace pax
