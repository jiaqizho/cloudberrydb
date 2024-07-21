#include "clustering/pax_clustering_writer.h"

#include "catalog/pax_aux_table.h"

namespace pax {
namespace clustering {

PaxClusteringWriter::PaxClusteringWriter(Relation rel)
    : rel_(rel), writer_(nullptr) {}

PaxClusteringWriter::~PaxClusteringWriter() { PAX_DELETE(writer_); }

void PaxClusteringWriter::WriteTuple(TupleTableSlot *tuple) {
  if (writer_ == nullptr) {
    writer_ = PAX_NEW<TableWriter>(rel_);
    writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
        ->SetFileSplitStrategy(PAX_NEW<PaxDefaultSplitStrategy>())
        ->Open();
  }
  writer_->WriteTuple(tuple);
  // TODO(gongxun): should update index if rel has index
}

void PaxClusteringWriter::Close() {
  if (writer_) {
    writer_->Close();
  }
}

}  // namespace clustering

}  // namespace pax