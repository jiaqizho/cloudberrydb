#pragma once

namespace pax {

#define VEC_BATCH_LENGTH (16384)
#define MEMORY_ALIGN_SIZE (8)
#define PAX_DATA_NO_ALIGN (1)

#define BITS_TO_BYTES(bits) (((bits) + 7) / 8)

#define COLUMN_STORAGE_FORMAT_IS_VEC(column) \
  (((column)->GetStorageFormat()) == PaxStorageFormat::kTypeStorageOrcVec)

enum PaxStorageFormat {
  // default non-vec store
  // which split null field and null bitmap
  kTypeStorageOrcNonVec,
  // vec storage format
  // spec the storage format
  kTypeStorageOrcVec,
};

}  // namespace pax
