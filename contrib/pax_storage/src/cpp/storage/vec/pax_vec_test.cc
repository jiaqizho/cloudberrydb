#include "comm/gtest_wrappers.h"
#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}

#pragma GCC diagnostic pop
#include "storage/vec/arrow_wrapper.h"
#endif

namespace pax::tests {

#ifdef VEC_BUILD

static void CreateOrcTestResourceOwner() {
  CurrentResourceOwner = ResourceOwnerCreate(NULL, "PaxVecTestResourceOwner");
}

static void ReleaseOrcTestResourceOwner() {
  ResourceOwner tmp_resource_owner = CurrentResourceOwner;
  CurrentResourceOwner = NULL;
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false,
                       true);
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_LOCKS, false, true);
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_AFTER_LOCKS, false,
                       true);
  ResourceOwnerDelete(tmp_resource_owner);
}

class PaxVecTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() override {
    MemoryContext pax_vec_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxVecTestMemoryContext", 80 * 1024 * 1024,
        80 * 1024 * 1024, 80 * 1024 * 1024);

    MemoryContextSwitchTo(pax_vec_test_memory_context);
    CreateOrcTestResourceOwner();
  }

  static CTupleSlot *CreateCtuple(bool is_fixed) {
    TupleTableSlot *tuple_slot;
    TupleDescData *tuple_desc;
    CTupleSlot *ctuple_slot;

    tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 1));

    tuple_desc->natts = 1;
    if (is_fixed) {
      tuple_desc->attrs[0] = {
          .atttypid = INT4OID,
          .attlen = 4,
          .attbyval = true,
      };
    } else {
      tuple_desc->attrs[0] = {
          .atttypid = TEXTOID,
          .attlen = -1,
          .attbyval = false,
      };
    }

    tuple_slot = (TupleTableSlot *)cbdb::RePalloc(
        MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual),
        MAXALIGN(TTSOpsVirtual.base_slot_size) +
            MAXALIGN(tuple_desc->natts * sizeof(Datum)) +
            MAXALIGN(tuple_desc->natts * sizeof(bool)) +
            MAXALIGN(sizeof(VecTupleTableSlot)));

    tuple_slot->tts_tupleDescriptor = tuple_desc;
    ctuple_slot = new CTupleSlot(tuple_slot);

    return ctuple_slot;
  }

  static void DeleteCTupleSlot(CTupleSlot *ctuple_slot) {
    auto tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    cbdb::Pfree(tuple_table_slot->tts_tupleDescriptor);
    cbdb::Pfree(tuple_table_slot);
    delete ctuple_slot;
  }

  void TearDown() override { ReleaseOrcTestResourceOwner(); }
};

TEST_P(PaxVecTest, PaxColumnToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(VEC_BATCH_LENGTH, flush_counts);

  // verify ctuple_slot 1
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
                VEC_BATCH_LENGTH * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  // verify ctuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i + VEC_BATCH_LENGTH);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i + VEC_BATCH_LENGTH);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + 1000 * sizeof(int32))),
                1000 * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnWithNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  CTupleSlot *ctuple_slot;
  size_t null_counts = 0;
  auto is_fixed = GetParam();

  ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (i % 5 == 0) {
      null_counts++;
      column->AppendNull();
    }

    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);

  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(VEC_BATCH_LENGTH, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(
        child_array->null_count,
        VEC_BATCH_LENGTH - column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        // N 0 1 2 3 4 N 5 6 7 8 9 N 10 11 ...
        // should % 6 rather then 5
        if (i % 6 == 0) {
          ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
        } else {
          ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
        }
      }

      // verify data
      char *buffer = (char *)child_array->buffers[1];
      size_t verify_null_counts = 0;
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          verify_null_counts++;
          continue;
        }
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i - verify_null_counts);
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
        } else {
          ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
        }
      }

      // verify offset data
      char *offset_buffer = (char *)child_array->buffers[1];
      size_t last_offset = 0;
      size_t verify_null_counts = 0;
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          verify_null_counts++;
          ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                    last_offset == 0 ? 0 : last_offset + sizeof(int32));
          continue;
        }
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  (i - verify_null_counts) * sizeof(int32));
        last_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
                last_offset + sizeof(int32));
      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < VEC_BATCH_LENGTH - verify_null_counts; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(null_counts + 1000, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    size_t range_size = null_counts + 1000;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, range_size);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, range_size);
    ASSERT_EQ(
        child_array->null_count,
        range_size - column->GetRangeNonNullRows(VEC_BATCH_LENGTH, range_size));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < range_size; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      // verify null bitmap
      auto null_bits_array = (uint8 *)child_array->buffers[0];

      size_t verify_null_counts = 0;
      for (size_t i = 0; i < range_size; i++) {
        if (!arrow::bit_util::GetBit(null_bits_array, i)) {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < (range_size - child_array->null_count); i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
      }

      // verify offset with data
      char *offset_buffer = (char *)child_array->buffers[1];
      start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);

      verify_null_counts = 0;
      for (size_t i = 0; i < range_size; i++) {
        auto current_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
        auto next_offset =
            *((int32 *)(offset_buffer + (i + 1) * sizeof(int32)));
        if (current_offset != next_offset) {
          ASSERT_EQ(
              *((int32 *)(buffer + (i - verify_null_counts) * sizeof(int32))),
              start++);
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // append finish
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  // verify ctuple_slot
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + 1000 * sizeof(int32))),
                1000 * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnWithNullToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  size_t null_counts = 0;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    if (i % 5 == 0) {
      null_counts++;
      column->AppendNull();
    }

    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }
  ASSERT_EQ(column->GetRows() - column->GetNonNullRows(), null_counts);
  ASSERT_EQ(column->GetNonNullRows(), 1000);

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000 + null_counts, flush_counts);

  // verify ctuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, 1000 + null_counts);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000 + null_counts);
    ASSERT_EQ(child_array->null_count, null_counts);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(start, 1000);
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      // verify null bitmap
      auto null_bits_array = (uint8 *)child_array->buffers[0];

      size_t verify_null_counts = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        if (!arrow::bit_util::GetBit(null_bits_array, i)) {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = 0;
      for (int64 i = 0; i < (child_array->length - child_array->null_count);
           i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
      }
      ASSERT_EQ(start, 1000);

      // verify offset with data
      char *offset_buffer = (char *)child_array->buffers[1];
      start = 0;

      verify_null_counts = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        auto current_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
        auto next_offset =
            *((int32 *)(offset_buffer + (i + 1) * sizeof(int32)));
        if (current_offset != next_offset) {
          ASSERT_EQ(
              *((int32 *)(buffer + (i - verify_null_counts) * sizeof(int32))),
              start++);
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(start, 1000);
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnAllNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(1000);
  } else {
    column = new PaxNonFixedColumn(1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    column->AppendNull();
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, arrow_array->buffers);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 1000);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
      }

    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
      }

      char *offset_buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i <= 1000; i++) {
        // all of offset is 0
        // no data in data part
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))), 0);
      }
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
    ASSERT_EQ(child_array->private_data, child_array->buffers);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

INSTANTIATE_TEST_CASE_P(PaxVecTestCombine, PaxVecTest,
                        testing::Values(true, false));

#endif

}  // namespace pax::tests