#include "storage/orc/orc_group.h"
#include "storage/toast/pax_toast.h"
namespace pax {

OrcVecGroup::OrcVecGroup(std::unique_ptr<PaxColumns> &&pax_column,
                         size_t row_offset,
                         const std::vector<int> *proj_col_index)
    : OrcGroup(std::move(pax_column), row_offset, proj_col_index) {
  Assert(COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns_));
}

static std::pair<Datum, std::shared_ptr<MemoryObject>> GetDatumWithNonNull(const std::shared_ptr<PaxColumn> &column,
                                                   size_t row_index) {
  Datum datum = 0;
  std::shared_ptr<MemoryObject> ref;
  char *buffer;
  size_t buffer_len;

  Assert(column);

  std::tie(buffer, buffer_len) = column->GetBuffer(row_index);
  switch (column->GetPaxColumnTypeInMem()) {
    case kTypeVecBpChar:
    case kTypeNonFixed:
      datum = PointerGetDatum(buffer);
      if (column->IsToast(row_index)) {
        auto external_buffer = column->GetExternalToastDataBuffer();
        std::tie(datum, ref) = pax_detoast(datum,
                          external_buffer ? external_buffer->Start() : nullptr,
                          external_buffer ? external_buffer->Used() : 0);
        break;
      }

      {
        auto size = TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len + VARHDRSZ);
        ByteBuffer bb(size, size);
        auto tmp = bb.Addr();
        SET_VARSIZE(tmp, buffer_len + VARHDRSZ);
        memcpy(VARDATA(tmp), buffer, buffer_len);
        datum = PointerGetDatum(tmp);
        ref = std::make_shared<ExternalToastValue>(std::move(bb));
      }
      break;
    case kTypeVecBitPacked:
    case kTypeFixed: {
      Assert(buffer_len > 0);
      switch (buffer_len) {
        case 1:
          datum = cbdb::Int8ToDatum(*reinterpret_cast<int8 *>(buffer));
          break;
        case 2:
          datum = cbdb::Int16ToDatum(*reinterpret_cast<int16 *>(buffer));
          break;
        case 4:
          datum = cbdb::Int32ToDatum(*reinterpret_cast<int32 *>(buffer));
          break;
        case 8:
          datum = cbdb::Int64ToDatum(*reinterpret_cast<int64 *>(buffer));
          break;
        default:
          Assert(!"should't be here, fixed type len should be 1, 2, 4, 8");
      }
      break;
    }
    case kTypeVecDecimal:
    case kTypeVecNoHeader: {
      datum = PointerGetDatum(buffer);
      break;
    }
    case kTypeBitPacked:
    case kTypeBpChar:
    case kTypeDecimal:
    default:
      Assert(!"should't be here, non-implemented column type in memory");
      break;
  }

  return {datum, ref};
}

std::pair<Datum, bool> OrcVecGroup::GetColumnValue(const std::shared_ptr<PaxColumn> &column,
                                                   size_t row_index,
                                                   uint32 * /*null_counts*/) {
  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      return {0, true};
    }
  }
  Datum datum;
  std::shared_ptr<MemoryObject> mobj;

  std::tie(datum, mobj) = GetDatumWithNonNull(column, row_index);
  if (mobj) buffer_holders_.emplace_back(mobj);

  return {datum, false};
}

}  // namespace pax
