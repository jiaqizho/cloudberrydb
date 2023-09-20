#include "storage/columns/pax_column_cache.h"

#include <uuid/uuid.h>

#ifdef ENABLE_PLASMA
namespace pax {

struct PaxColumnsMeta {
  int16 type_len : 8;
  uint32 null_size : 32;
  uint32 data_size : 32;
  uint32 len_size : 32;
  uint32 rows : 32;
} __attribute__((__aligned__(8)));

static std::string BuildVecKey(const std::string &file_name,
                               const size_t column_index) {
  uuid_t uuid;

  CBDB_CHECK(uuid_parse(file_name.c_str(), uuid) == 0,
             cbdb::CException::ExType::kExTypeCError);
  std::string key((char *)uuid, 16);
  key.append(std::to_string((uint16)column_index));

  return key;
}

PaxColumnCache::PaxColumnCache(PaxCache *cache, const std::string &file_name,
                               bool *proj, size_t proj_num)
    : pax_cache_(cache),
      file_name_(file_name),
      proj_(proj),
      proj_num_(proj_num) {
  Assert(pax_cache_ && proj_);
}

template <typename T>
static PaxColumn *NewFixColumn(const char *buffer, size_t buffer_len) {
  auto column = new PaxCommColumn<T>(0);
  Assert(buffer_len % sizeof(T) == 0);
  auto data_buffer = new DataBuffer<T>((T *)(buffer), buffer_len, false, false);
  data_buffer->BrushAll();
  column->Set(data_buffer);
  return column;
}

std::tuple<PaxColumns *, std::vector<std::string>, bool *>
PaxColumnCache::ReadCache() {
  PaxColumns *columns = new PaxColumns();
  std::vector<std::string> keys;
  std::vector<PaxCache::BatchBuffer> batchs;
  size_t cache_index = 0;
  int64 rows = -1;
  bool *proj_copy = new bool[proj_num_];
  size_t no_proj_num = 0;

  memcpy(proj_copy, proj_, proj_num_);

  for (size_t i = 0; i < proj_num_; i++) {
    if (!proj_copy[i]) {
      continue;
    }
    keys.emplace_back(BuildVecKey(file_name_, i));
  }

  auto status = pax_cache_->Get(keys, batchs);
  if (!status.Ok()) {
    keys.clear();
    // TODO(jiaqizho): add log here
    return std::make_tuple(nullptr, keys, proj_copy);
  }

  for (size_t i = 0; i < proj_num_; i++) {
    if (!proj_copy[i]) {
      no_proj_num++;
      columns->Append(nullptr);
      continue;
    }
    auto batch_buffer = batchs[cache_index++];

    if (batch_buffer.not_exist) {
      keys[i - no_proj_num] = "";
      columns->Append(nullptr);
      continue;
    }

    Assert(batch_buffer.meta_len == sizeof(PaxColumnsMeta));
    PaxColumnsMeta *meta = (PaxColumnsMeta *)batch_buffer.meta;

    AssertImply(rows != -1, (size_t)rows == meta->rows);
    rows = meta->rows;

    DataBuffer<bool> *null_bitmap = nullptr;
    if (meta->null_size != 0) {
      null_bitmap = new DataBuffer((bool *)(batch_buffer.buffer),
                                   meta->null_size, false, false);
      null_bitmap->BrushAll();
    }

    Assert(batch_buffer.buffer_len ==
           (size_t)(meta->null_size + meta->data_size + meta->len_size));

    PaxColumn *column = nullptr;
    switch (meta->type_len) {
      case -1: {
        auto non_fixed_column = new PaxNonFixedColumn(0);
        Assert(meta->len_size % sizeof(int64) == 0);
        auto data_buffer = new DataBuffer<char>(
            (char *)(batch_buffer.buffer + meta->null_size), meta->data_size,
            false, false);
        auto len_data_buffer = new DataBuffer<int64>(
            (int64 *)(batch_buffer.buffer + meta->null_size + meta->data_size),
            meta->len_size, false, false);
        data_buffer->BrushAll();
        len_data_buffer->BrushAll();

        non_fixed_column->Set(data_buffer, len_data_buffer,
                              batch_buffer.buffer_len);
        column = non_fixed_column;
        break;
      }
      case 1: {
        column = NewFixColumn<int8>(batch_buffer.buffer + meta->null_size,
                                    meta->data_size);
        break;
      }
      case 2: {
        column = NewFixColumn<int16>(batch_buffer.buffer + meta->null_size,
                                     meta->data_size);
        break;
      }
      case 4: {
        column = NewFixColumn<int32>(batch_buffer.buffer + meta->null_size,
                                     meta->data_size);
        break;
      }
      case 8: {
        column = NewFixColumn<int64>(batch_buffer.buffer + meta->null_size,
                                     meta->data_size);
        break;
      }
      default: {
        Assert(false);
      }
    }

    if (null_bitmap) {
      column->SetNulls(null_bitmap);
    }

    columns->Append(column);
    proj_copy[i] = false;
  }

  if (rows != -1) {
    columns->AddRows(rows);
  }

  return std::make_tuple(columns, keys, proj_copy);
}

void PaxColumnCache::ReleaseCache(std::vector<std::string> keys) {
  for (auto &key : keys) {
    if (key.length() != 0) pax_cache_->Release(key);
  }
}

void PaxColumnCache::WriteCache(PaxColumns *columns) {
  std::string key;
  PaxColumnsMeta meta{};
  int64 rows = -1;

  for (size_t i = 0; i < proj_num_; i++) {
    auto column = (*columns)[i];
    if (!proj_[i] || !column) {
      continue;
    }

    key = BuildVecKey(file_name_, i);

    AssertImply(rows != -1, (size_t)rows == column->GetRows());
    rows = column->GetRows();

    std::vector<std::pair<char *, size_t>> buffers;

    DataBuffer<bool> *nulls = nullptr;
    if (column->HasNull()) {
      nulls = column->GetNulls();
      meta.null_size = nulls->Used();
      buffers.emplace_back(
          std::make_pair((char *)nulls->GetBuffer(), nulls->Used()));
    } else {
      meta.null_size = 0;
    }

    char *buffer = nullptr;
    size_t buffer_len = 0;

    if (column->GetPaxColumnTypeInMem() == kTypeNonFixed) {
      auto non_fixed_column = (PaxNonFixedColumn *)column;
      std::tie(buffer, buffer_len) = non_fixed_column->GetBuffer();
      auto len_buffer = non_fixed_column->GetLengthBuffer();

      buffers.emplace_back(std::make_pair(buffer, buffer_len));
      buffers.emplace_back(
          std::make_pair((char *)len_buffer->GetBuffer(), len_buffer->Used()));

      meta.type_len = non_fixed_column->GetTypeLength();
      meta.data_size = buffer_len;
      meta.len_size = len_buffer->Used();
      meta.rows = rows;
    } else if (column->GetPaxColumnTypeInMem() == kTypeFixed) {
      std::tie(buffer, buffer_len) = column->GetBuffer();
      buffers.emplace_back(std::make_pair(buffer, buffer_len));

      meta.type_len = column->GetTypeLength();
      meta.data_size = buffer_len;
      meta.len_size = 0;
      meta.rows = rows;

    } else {
      Assert(false);
    }

    pax_cache_->Put(key, buffers,
                    std::make_pair((char *)&meta, sizeof(PaxColumnsMeta)));
  }
}

}  // namespace pax

#endif  // ENABLE_PLASMA