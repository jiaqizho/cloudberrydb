#pragma once
#include <vector>

#include "storage/proto/proto_wrappers.h"

#include "storage/paxc_define.h"

namespace pax {

#define VEC_BATCH_LENGTH (16384)
#define MEMORY_ALIGN_SIZE (8)
#define PAX_DATA_NO_ALIGN (1)
#define BITS_TO_BYTES(bits) (((bits) + 7) / 8)

#define PAX_LENGTHS_DEFAULT_COMPRESSTYPE \
  ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD
#define PAX_LENGTHS_DEFAULT_COMPRESSLEVEL 5

#define COLUMN_STORAGE_FORMAT_IS_VEC(column) \
  (((column)->GetStorageFormat()) == PaxStorageFormat::kTypeStoragePorcVec)

enum PaxStorageFormat {
  // default non-vec store
  // which split null field and null bitmap
  kTypeStoragePorcNonVec = 1,
  // vec storage format
  // spec the storage format
  kTypeStoragePorcVec = 2,
};

}  // namespace pax
