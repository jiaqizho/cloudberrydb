#include "storage/pax.h"

#include <string>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc_native_micro_partition.h"

namespace pax::tests {
const char *pax_file_name = "./test.pax";
#define COLUMN_NUMS 2

TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc,
                                   const TupleTableSlotOps *tts_ops) {
  Size basesz, allocsz;
  TupleTableSlot *slot;

  basesz = tts_ops->base_slot_size;

  if (tupleDesc)
    allocsz = MAXALIGN(basesz) + MAXALIGN(tupleDesc->natts * sizeof(Datum)) +
              MAXALIGN(tupleDesc->natts * sizeof(bool));
  else
    allocsz = basesz;

  slot = reinterpret_cast<TupleTableSlot *>(cbdb::Palloc0(allocsz));
  *((const TupleTableSlotOps **)&slot->tts_ops) = tts_ops;
  slot->type = T_TupleTableSlot;
  slot->tts_flags |= TTS_FLAG_EMPTY;
  if (tupleDesc != NULL) slot->tts_flags |= TTS_FLAG_FIXED;
  slot->tts_tupleDescriptor = tupleDesc;
  slot->tts_mcxt = CurrentMemoryContext;
  slot->tts_nvalid = 0;

  if (tupleDesc != NULL) {
    slot->tts_values = reinterpret_cast<Datum *>(
        (reinterpret_cast<char *>(slot)) + MAXALIGN(basesz));
    slot->tts_isnull = reinterpret_cast<bool *>(
        (reinterpret_cast<char *>(slot)) + MAXALIGN(basesz) +
        MAXALIGN(tupleDesc->natts * sizeof(Datum)));
  }
  slot->tts_ops->init(slot);

  return slot;
}

CTupleSlot *CreateFakeCTupleSlot(bool with_value) {
  TupleTableSlot *tuple_slot;

  TupleDescData *tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
      sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));

  tuple_desc->natts = COLUMN_NUMS;
  tuple_desc->attrs[0] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_desc->attrs[1] = {
      .attlen = 4,
      .attbyval = true,
  };

  tuple_slot = pax::tests::MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  if (with_value) {
    bool *fake_is_null = new bool[2];

    fake_is_null[0] = false;
    fake_is_null[1] = false;

    tuple_slot->tts_values[0] = Int32GetDatum(1);
    tuple_slot->tts_values[1] = Int32GetDatum(2);
    tuple_slot->tts_isnull = fake_is_null;
  }

  CTupleSlot *ctuple_slot = new CTupleSlot(tuple_slot);

  return ctuple_slot;
}

class MockReaderInterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  MockReaderInterator(const std::vector<std::shared_ptr<MicroPartitionMetadata>>
                          &meta_info_list)
      : index(0) {
    micro_partitions_ = std::make_shared<
        std::vector<std::shared_ptr<MicroPartitionMetadata>>>();
    micro_partitions_->insert(micro_partitions_->end(), meta_info_list.begin(),
                              meta_info_list.end());
  }
  void Init() override{};
  bool HasNext() override { return index < micro_partitions_->size(); };
  std::shared_ptr<MicroPartitionMetadata> &Next() override {
    return micro_partitions_->at(index++);
  };
  void Seek(int offset, IteratorSeekPosType whence) {}

 private:
  uint32_t index;
  std::shared_ptr<std::vector<std::shared_ptr<MicroPartitionMetadata>>>
      micro_partitions_;
};

class PaxWriterTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override { std::remove(pax_file_name); }

 protected:
  // port from postgresql
};

TEST_F(PaxWriterTest, WriteAndReadTuple) {
  pax::MicroPartitionWriter::WriterOptions options;
  options.file_name = pax_file_name;
  CTupleSlot *slot = CreateFakeCTupleSlot(true);
  options.desc = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  options.buffer_size = 64 * 1024;

  pax::FileSystem *fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
  MicroPartitionWriter *micro_partition_writer =
      new OrcNativeMicroPartitionWriter(options, fs);
  micro_partition_writer->SetWriteSummaryCallback(
      [](const pax::WriteSummary &summary) {});

  TableWriter *writer = new TableWriter(micro_partition_writer);
  writer->Open();

  writer->WriteTuple(slot);
  ASSERT_EQ(1, writer->total_tuples());
  writer->Close();

  cbdb::Pfree(slot->GetTupleTableSlot());
  delete writer;

  FileSystemPtr file_system = Singleton<LocalFileSystem>::GetInstance();

  MicroPartitionReaderPtr micro_partition_reader =
      new OrcNativeMicroPartitionReader(file_system);

  std::vector<std::shared_ptr<MicroPartitionMetadata>> meta_info_list;
  std::shared_ptr<MicroPartitionMetadata> meta_info_ptr =
      std::make_shared<MicroPartitionMetadata>(pax_file_name, pax_file_name);

  meta_info_list.push_back(meta_info_ptr);

  std::shared_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::shared_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  reader = new TableReader(micro_partition_reader, meta_info_iterator);
  reader->Open();

  CTupleSlot *rslot = CreateFakeCTupleSlot(false);

  reader->ReadTuple(rslot);

  ASSERT_EQ(1, DatumGetInt32(rslot->GetTupleTableSlot()->tts_values[0]));
  ASSERT_EQ(2, DatumGetInt32(rslot->GetTupleTableSlot()->tts_values[1]));
}

}  // namespace pax::tests
