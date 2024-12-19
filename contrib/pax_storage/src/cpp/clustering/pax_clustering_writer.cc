#include "clustering/pax_clustering_writer.h"

#include "catalog/pax_catalog.h"

namespace pax {
namespace clustering {

static void InsertOrUpdateClusteredMicroPartitionEntry(
    const pax::WriteSummary &summary) {
  pax::WriteSummary clusterd_summary(summary);
  clusterd_summary.is_clustered = true;
  cbdb::InsertOrUpdateMicroPartitionEntry(clusterd_summary);
}

PaxClusteringWriter::PaxClusteringWriter(Relation rel)
    : rel_(rel) {}

PaxClusteringWriter::~PaxClusteringWriter() { }

void PaxClusteringWriter::WriteTuple(TupleTableSlot *tuple) {
  if (writer_ == nullptr) {
    writer_ = std::make_unique<TableWriter>(rel_);
    writer_->SetWriteSummaryCallback(InsertOrUpdateClusteredMicroPartitionEntry)
        ->SetFileSplitStrategy(std::make_unique<PaxDefaultSplitStrategy>())
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
