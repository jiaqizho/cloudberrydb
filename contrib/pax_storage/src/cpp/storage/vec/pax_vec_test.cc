#include "comm/gtest_wrappers.h"
#include "comm/singleton.h"
#include "pax_gtest_helper.h"
#include "storage/columns/pax_vec_numeric_column.h"
#include "storage/local_file_system.h"
#include "storage/pax.h"
#include "storage/vec/pax_vec_adapter.h"
#ifdef VEC_BUILD
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}

#pragma GCC diagnostic pop
#include "storage/vec/arrow_wrapper.h"
#endif  // VEC_BUILD

namespace pax::tests {

#ifdef VEC_BUILD
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

static TupleTableSlotOps TTSOpsVecTuple;
static void GenFakeBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

class PaxVecTest
    : public ::testing::TestWithParam<::testing::tuple<bool, bool>> {
 public:
  void SetUp() override {
    memcpy(&TTSOpsVecTuple, &TTSOpsVirtual, sizeof(TTSOpsVirtual));
    TTSOpsVecTuple.base_slot_size = sizeof(VecTupleTableSlot);

    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  static TupleTableSlot *CreateDecimalTupleSlot(bool with_value = false) {
    TupleDescData *tuple_desc;
    TupleTableSlot *tuple_slot;

    tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 1));

    tuple_desc->natts = 1;
    tuple_desc->attrs[0] = {.atttypid = NUMERICOID,
                            .attlen = -1,
                            .attbyval = false,
                            .attalign = TYPALIGN_DOUBLE,
                            .attstorage = TYPSTORAGE_PLAIN,
                            .attcollation = DEFAULT_COLLATION_OID};

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVecTuple);

    if (with_value) {
      auto numeric = int64_to_numeric(1000);
      tuple_slot->tts_values[0] = NumericGetDatum(numeric);

      bool *fake_is_null =
          reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool)));
      fake_is_null[0] = false;
      tuple_slot->tts_isnull = fake_is_null;
    }

    tuple_slot->tts_tupleDescriptor = tuple_desc;
    return tuple_slot;
  }

  static TupleTableSlot *CreateTupleSlot(bool is_fixed, bool with_value = false,
                                         bool with_ctid = false) {
    TupleDescData *tuple_desc;
    TupleTableSlot *tuple_slot;

    int attnums = 1;
    if (with_ctid) {
      attnums++;
    }

    tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * attnums));

    tuple_desc->natts = attnums;
    if (is_fixed) {
      tuple_desc->attrs[0] = {.atttypid = INT4OID,
                              .attlen = 4,
                              .attbyval = true,
                              .attalign = TYPALIGN_INT,
                              .attstorage = TYPSTORAGE_PLAIN,
                              .attcollation = InvalidOid};
    } else {
      tuple_desc->attrs[0] = {.atttypid = TEXTOID,
                              .attlen = -1,
                              .attbyval = false,
                              .attalign = TYPALIGN_DOUBLE,
                              .attstorage = TYPSTORAGE_PLAIN,
                              .attcollation = DEFAULT_COLLATION_OID};
    }

    if (with_ctid) {
      tuple_desc->attrs[1] = {.atttypid = TEXTOID,
                              .attlen = 8,
                              .attbyval = true,
                              .attalign = TYPALIGN_DOUBLE,
                              .attstorage = TYPSTORAGE_PLAIN,
                              .attcollation = DEFAULT_COLLATION_OID};
      const char *blockname = "ctid";
      namestrcpy(&tuple_desc->attrs[1].attname, blockname);
    }

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVecTuple);

    if (with_value) {
      if (is_fixed) {
        tuple_slot->tts_values[0] = cbdb::Int32ToDatum(0x123);
      } else {
        char column_buff[100];
        GenFakeBuffer(column_buff, 100);
        tuple_slot->tts_values[0] = cbdb::DatumFromCString(column_buff, 100);
      }

      bool *fake_is_null =
          reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool)));
      fake_is_null[0] = false;
      tuple_slot->tts_isnull = fake_is_null;
    }

    tuple_slot->tts_tupleDescriptor = tuple_desc;
    return tuple_slot;
  }

  static void DeleteTupleSlot(TupleTableSlot *tuple_slot) {
    ExecDropSingleTupleTableSlot(tuple_slot);
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

 protected:
  const char *file_name_ = "9000";
};

TEST_P(PaxVecTest, PaxColumnToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = ::testing::get<0>(GetParam());
  auto with_visimap = ::testing::get<1>(GetParam());
  auto tuple_slot = CreateTupleSlot(is_fixed);
  std::shared_ptr<Bitmap8> visimap = nullptr;
  if (with_visimap) {
    visimap = std::make_shared<Bitmap8>(VEC_BATCH_LENGTH + 1000);
  }

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (with_visimap) {
      if (i % 2 == 0) {
        visimap->Set(i);
      }
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
  adapter->SetDataSource(columns, 0);
  if (with_visimap) {
    adapter->SetVisibitilyMapInfo(visimap);
  }

  size_t visiable_tuple1_count = VEC_BATCH_LENGTH;
  if (with_visimap) {
    visiable_tuple1_count = VEC_BATCH_LENGTH / 2;
  }

  auto append_rc = adapter->AppendToVecBuffer();

  ASSERT_EQ(static_cast<size_t>(append_rc), visiable_tuple1_count);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);

  ASSERT_EQ(visiable_tuple1_count, flush_counts);

  // verify tuple_slot 1
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), visiable_tuple1_count);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(static_cast<size_t>(child_array->length), visiable_tuple1_count);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < visiable_tuple1_count; i++) {
        if (with_visimap) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i * 2 + 1));
        } else {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i));
        }
      }

    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      if (with_visimap) {
        for (size_t i = 0; i < visiable_tuple1_count; i++) {
          ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                    static_cast<int32>(i * sizeof(int32)));
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i * 2 + 1));
        }
      } else {
        for (size_t i = 0; i < visiable_tuple1_count; i++) {
          ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                    static_cast<int32>(i * sizeof(int32)));
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i));
        }
      }

      if (with_visimap) {
        ASSERT_EQ(
            *((int32 *)(offset_buffer + VEC_BATCH_LENGTH / 2 * sizeof(int32))),
            static_cast<int32>(VEC_BATCH_LENGTH / 2 * sizeof(int32)));
      } else {
        ASSERT_EQ(
            *((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
            static_cast<int32>(VEC_BATCH_LENGTH * sizeof(int32)));
      }
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  size_t visiable_tuple2_count = 1000;
  if (with_visimap) {
    visiable_tuple2_count = 500;
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(static_cast<size_t>(append_rc), visiable_tuple2_count);

  flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(visiable_tuple2_count, flush_counts);

  // verify tuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), visiable_tuple2_count);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(static_cast<size_t>(child_array->length), visiable_tuple2_count);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < visiable_tuple2_count; i++) {
        if (with_visimap) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>((i + visiable_tuple1_count) * 2 + 1));
        } else {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i + visiable_tuple1_count));
        }
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < visiable_tuple2_count; i++) {
        if (with_visimap) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>((i + visiable_tuple1_count) * 2 + 1));
        } else {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(i + visiable_tuple1_count));
        }
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  static_cast<int32>(i * sizeof(int32)));
      }

      ASSERT_EQ(
          *((int32 *)(offset_buffer + visiable_tuple2_count * sizeof(int32))),
          static_cast<int32>(visiable_tuple2_count * sizeof(int32)));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnWithNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  size_t null_counts = 0;
  auto is_fixed = ::testing::get<0>(GetParam());

  TupleTableSlot *tuple_slot = CreateTupleSlot(is_fixed);

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
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
  adapter->SetDataSource(columns, 0);

  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, VEC_BATCH_LENGTH);

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(static_cast<size_t>(VEC_BATCH_LENGTH), flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(
        static_cast<size_t>(child_array->null_count),
        VEC_BATCH_LENGTH - column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

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
                  static_cast<int32>(i - verify_null_counts));
      }

      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
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
                    static_cast<int32>(
                        last_offset == 0 ? 0 : last_offset + sizeof(int32)));
          continue;
        }
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  static_cast<int32>((i - verify_null_counts) * sizeof(int32)));
        last_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
                static_cast<int32>(last_offset + sizeof(int32)));
      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < VEC_BATCH_LENGTH - verify_null_counts; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  static_cast<int32>(i));
      }

      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(static_cast<size_t>(append_rc), 1000 + null_counts);

  flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(null_counts + 1000, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    size_t range_size = null_counts + 1000;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), range_size);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(static_cast<size_t>(child_array->length), range_size);
    ASSERT_EQ(
        static_cast<size_t>(child_array->null_count),
        range_size - column->GetRangeNonNullRows(VEC_BATCH_LENGTH, range_size));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < range_size; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(start++));
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
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

      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < (range_size - child_array->null_count); i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  static_cast<int32>(start++));
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
              static_cast<int32>(start++));
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = ::testing::get<0>(GetParam());
  auto tuple_slot = CreateTupleSlot(is_fixed);

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
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
  adapter->SetDataSource(columns, 0);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, 1000);

  // append finish
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(1000UL, flush_counts);

  // verify tuple_slot
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  static_cast<int32>(i));
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  static_cast<int32>(i * sizeof(int32)));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  static_cast<int32>(i));
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + 1000 * sizeof(int32))),
                static_cast<int32>(1000 * sizeof(int32)));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnWithNullToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  size_t null_counts = 0;

  auto is_fixed = ::testing::get<0>(GetParam());
  auto tuple_slot = CreateTupleSlot(is_fixed);

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
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
  ASSERT_EQ(column->GetNonNullRows(), 1000UL);

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns, 0);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(static_cast<size_t>(append_rc), 1000 + null_counts);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(1000 + null_counts, flush_counts);

  // verify tuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), 1000 + null_counts);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(static_cast<size_t>(child_array->length), 1000 + null_counts);
    ASSERT_EQ(static_cast<size_t>(child_array->null_count), null_counts);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                    static_cast<int32>(start++));
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(start, 1000UL);
      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
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

      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = 0;
      for (int64 i = 0; i < (child_array->length - child_array->null_count);
           i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  static_cast<int32>(start++));
      }
      ASSERT_EQ(start, 1000UL);

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
              static_cast<int32>(start++));
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(start, 1000UL);
      ASSERT_EQ(verify_null_counts,
                static_cast<size_t>(child_array->null_count));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnAllNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = ::testing::get<0>(GetParam());
  auto tuple_slot = CreateTupleSlot(is_fixed);

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(1000, 1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    column->AppendNull();
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns, 0);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, 1000);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(1000UL, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 1000);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

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
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, DecimalTest) {
  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = ColumnEncoding_Kind_NO_ENCODED;

  TupleTableSlot *tuple_slot = CreateDecimalTupleSlot();

  auto adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  auto columns = PAX_NEW<PaxColumns>();
  auto column =
      PAX_NEW<PaxShortNumericColumn>(VEC_BATCH_LENGTH + 1000, encoding_option);

  for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
    auto numeric = int64_to_numeric(i);
    column->Append((char *)numeric,
                   NUMERIC_NDIGITS(numeric) * sizeof(NumericDigit) +
                       sizeof(int16) + VARHDRSZ);
  }
  columns->SetStorageFormat(PaxStorageFormat::kTypeStoragePorcVec);
  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns, 0);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, VEC_BATCH_LENGTH);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(static_cast<size_t>(VEC_BATCH_LENGTH), flush_counts);

  // verify tuple_slot 1
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, 2);  // decimal always 2
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    ASSERT_NE(child_array->buffers[1], nullptr);

    char *buffer = (char *)child_array->buffers[1];
    for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
      ASSERT_EQ(*((int64 *)(buffer + (i * sizeof(int64) * 2))),
                static_cast<int64>(i));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnWithNullAndVisimapToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  size_t null_counts = 0;
  auto is_fixed = ::testing::get<0>(GetParam());
  auto with_visimap = ::testing::get<1>(GetParam());
  std::shared_ptr<Bitmap8> visimap = nullptr;
  if (with_visimap) {
    visimap = std::make_shared<Bitmap8>(VEC_BATCH_LENGTH + 1000);
  }

  TupleTableSlot *tuple_slot = CreateTupleSlot(is_fixed);

  adapter =
      PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor, VEC_BATCH_LENGTH);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (i % 4 == 0) {
      null_counts++;
      column->AppendNull();
    }

    if (with_visimap) {
      // visimap:[1,0,1,0,1,0]
      if (i % 2 == 0) {
        visimap->Set(i);
      }
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
  adapter->SetDataSource(columns, 0);
  if (with_visimap) {
    adapter->SetVisibitilyMapInfo(visimap);
  }

  size_t visiable_tuple1_count = VEC_BATCH_LENGTH;
  if (with_visimap) {
    visiable_tuple1_count = VEC_BATCH_LENGTH / 2;
  }

  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(static_cast<size_t>(append_rc), visiable_tuple1_count);

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);
  ASSERT_EQ(visiable_tuple1_count, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), visiable_tuple1_count);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(static_cast<size_t>(child_array->length), visiable_tuple1_count);
    if (with_visimap) {
      ASSERT_EQ(child_array->null_count,
                VEC_BATCH_LENGTH / 5 - VEC_BATCH_LENGTH / 10);
    } else {
      ASSERT_EQ(static_cast<size_t>(child_array->null_count),
                visiable_tuple1_count / 5 + 1);
    }
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < visiable_tuple1_count; i++) {
        // nullmap: 0 1 1 1 1 0 1 1 1 1 0 1 1 1  1  0...
        // data   : n 0 1 2 3 n 4 5 6 7 n 8 9 10 11 n 12 13 14 15 n 16 ...
        // visimap: 1 0 1 0 1 0 1 0 1 0 1 0 1 0  1  0  1  0  1  0 1  0 ...
        // m_data : 0 2 n 5 7 8 10 n 13 15 16 18 n 21 23
        if (with_visimap) {
          if (i % 5 == 2) {
            ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
          } else {
            ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
          }
        } else {
          if (i % 5 == 0) {
            ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
          } else {
            ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
          }
        }
      }

    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < visiable_tuple1_count; i++) {
        if (with_visimap) {
          if (i % 5 == 2) {
            ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
          } else {
            ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
          }
        } else {
          if (i % 5 == 0) {
            ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
          } else {
            ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
          }
        }
      }
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }
  DeleteTupleSlot(tuple_slot);

  PAX_DELETE(columns);
  PAX_DELETE(adapter);
}

TEST_P(PaxVecTest, PaxColumnBuildCtidToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = ::testing::get<0>(GetParam());
  auto with_visimap = ::testing::get<1>(GetParam());
  auto tuple_slot = CreateTupleSlot(is_fixed, false, true);
  std::shared_ptr<Bitmap8> visimap = nullptr;
  if (with_visimap) {
    visimap = std::make_shared<Bitmap8>(VEC_BATCH_LENGTH + 1000);
  }

  adapter = PAX_NEW<VecAdapter>(tuple_slot->tts_tupleDescriptor,
                                VEC_BATCH_LENGTH, true);
  columns = PAX_NEW<PaxColumns>();
  if (is_fixed) {
    column = PAX_NEW<PaxCommColumn<int32>>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = PAX_NEW<PaxNonFixedColumn>(VEC_BATCH_LENGTH + 1000,
                                        VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (with_visimap) {
      if (i % 2 == 0) {
        visimap->Set(i);
      }
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
  adapter->SetDataSource(columns, 0);
  if (with_visimap) {
    adapter->SetVisibitilyMapInfo(visimap);
  }

  size_t visiable_tuple1_count = VEC_BATCH_LENGTH;
  if (with_visimap) {
    visiable_tuple1_count = VEC_BATCH_LENGTH / 2;
  }

  auto append_rc = adapter->AppendToVecBuffer();

  ASSERT_EQ(static_cast<size_t>(append_rc), visiable_tuple1_count);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_EQ(append_rc, -1);

  size_t flush_counts = adapter->FlushVecBuffer(tuple_slot);

  ASSERT_EQ(visiable_tuple1_count, flush_counts);

  // verify tuple_slot 1
  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(static_cast<size_t>(arrow_array->length), visiable_tuple1_count);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 2);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *ctid_child_array = arrow_array->children[1];
    ASSERT_EQ(static_cast<size_t>(ctid_child_array->length),
              visiable_tuple1_count);
    ASSERT_EQ(ctid_child_array->null_count, 0);
    ASSERT_EQ(ctid_child_array->offset, 0);
    ASSERT_EQ(ctid_child_array->n_buffers, 3);
    ASSERT_EQ(ctid_child_array->n_children, 0);
    ASSERT_EQ(ctid_child_array->children, nullptr);
    ASSERT_EQ(ctid_child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(ctid_child_array->buffers[1], nullptr);
    ASSERT_EQ(ctid_child_array->private_data, ctid_child_array);

    ASSERT_NE(ctid_child_array->buffers[2], nullptr);

    char *buffer = (char *)ctid_child_array->buffers[2];
    for (size_t i = 0; i < visiable_tuple1_count; i++) {
      if (with_visimap) {
        ASSERT_EQ(*(int64 *)(buffer + (i * sizeof(int64))),
                  static_cast<int64>((i + 1) * 2));
      } else {
        ASSERT_EQ(*(int64 *)(buffer + (i * sizeof(int64))),
                  static_cast<int64>(i + 1));
      }
    }

    ASSERT_EQ(ctid_child_array->dictionary, nullptr);
  }
}

class MockTableWriter : public TableWriter {
 public:
  MockTableWriter(const Relation relation, WriteSummaryCallback callback)
      : TableWriter(relation) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(PAX_NEW<PaxDefaultSplitStrategy>());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

class MockReaderInterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  explicit MockReaderInterator(
      const std::vector<MicroPartitionMetadata> &meta_info_list)
      : index_(0) {
    micro_partitions_.insert(micro_partitions_.end(), meta_info_list.begin(),
                             meta_info_list.end());
  }

  bool HasNext() override { return index_ < micro_partitions_.size(); }

  void Rewind() override { index_ = 0; }

  MicroPartitionMetadata Next() override { return micro_partitions_[index_++]; }

 private:
  uint32 index_;
  std::vector<MicroPartitionMetadata> micro_partitions_;
};

TEST_P(PaxVecTest, PaxVecReaderTest) {
  auto is_fixed = ::testing::get<0>(GetParam());
  TupleTableSlot *tuple_slot = CreateTupleSlot(is_fixed, true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = tuple_slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = PAX_NEW<MockTableWriter>(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(file_name_));
  encoding_opts.emplace_back(
      std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    writer->WriteTuple(tuple_slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTupleSlot(tuple_slot);
  PAX_DELETE(writer);

  tuple_slot = CreateTupleSlot(is_fixed);

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(file_name_);
  meta_info.SetMicroPartitionId(file_name_);
  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          PAX_NEW<MockReaderInterator>(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.is_vec = true;
  reader_options.tuple_desc = tuple_slot->tts_tupleDescriptor;

  reader = PAX_NEW<TableReader>(std::move(meta_info_iterator), reader_options);
  reader->Open();

  bool ok = reader->ReadTuple(tuple_slot);
  ASSERT_TRUE(ok);

  ok = reader->ReadTuple(tuple_slot);
  ASSERT_TRUE(ok);
  ok = reader->ReadTuple(tuple_slot);
  ASSERT_FALSE(ok);

  reader->Close();
  DeleteTupleSlot(tuple_slot);
  PAX_DELETE(relation);

  PAX_DELETE(reader);
}

INSTANTIATE_TEST_SUITE_P(PaxVecTestCombine, PaxVecTest,
                         testing::Combine(testing::Values(true, false),
                                          testing::Values(true, false)));

#endif  // VEC_BUILD

}  // namespace pax::tests
