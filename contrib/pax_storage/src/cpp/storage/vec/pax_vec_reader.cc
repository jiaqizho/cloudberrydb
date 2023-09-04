#include "storage/vec/pax_vec_reader.h"

#include "storage/vec/pax_vec_adapter.h"
#ifdef VEC_BUILD

namespace pax {

PaxVecReader::PaxVecReader(MicroPartitionReader *reader, VecAdapter *adapter)
    : reader_(reader), adapter_(adapter) {}

PaxVecReader::~PaxVecReader() { delete reader_; }

void PaxVecReader::Open(const ReaderOptions &options) {
  reader_->Open(options);
  PaxColumns *pax_columns = reader_->GetAllColumns();
  adapter_->SetDataSource(pax_columns);
}

void PaxVecReader::Close() { reader_->Close(); }

bool PaxVecReader::ReadTuple(CTupleSlot *cslot) {
  if (!adapter_->AppendToVecBuffer()) {
    return false;
  }

  auto flush_nums_of_rows = adapter_->FlushVecBuffer(cslot);
  Assert(flush_nums_of_rows);
  return true;
}

PaxColumns *PaxVecReader::GetAllColumns() {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}
};  // namespace pax

#endif  // VEC_BUILD
