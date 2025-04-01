#include "comm/guc.h"

#include "comm/cbdb_api.h"

#include "storage/pax_defined.h"
#include "storage/pax_itemptr.h"

namespace pax {

#define PAX_SCAN_REUSE_BUFFER_DEFAULT_SIZE (8 * 1024 * 1024)
#define PAX_SCAN_REUSE_BUFFER_MIN_SIZE (1 * 1024 * 1024)
#define PAX_SCAN_REUSE_BUFFER_MAX_SIZE (32 * 1024 * 1024)

#define PAX_MAX_TUPLES_PER_GROUP_DEFAULT (VEC_BATCH_LENGTH * 8)
#define PAX_MAX_TUPLES_PER_GROUP_MIN (5)
#define PAX_MAX_TUPLES_PER_GROUP_MAX (VEC_BATCH_LENGTH * 32)

#define PAX_MAX_TUPLES_PER_FILE_DEFAULT (VEC_BATCH_LENGTH * 80)
#define PAX_MAX_TUPLES_PER_FILE_MIN (VEC_BATCH_LENGTH * 8)

#define PAX_MAX_SIZE_PER_FILE_DEFAULT (64 * 1024 * 1024)
#define PAX_MAX_SIZE_PER_FILE_MIN (8 * 1024 * 1024)
#define PAX_MAX_SIZE_PER_FILE_MAX (320 * 1024 * 1024)

#define PAX_MIN_SIZE_MAKE_COMPRESSED_TOAST (512 * 1024)
#define PAX_MAX_SIZE_MAKE_COMPRESSED_TOAST (1U << VARLENA_EXTSIZE_BITS)
#define PAX_MIN_SIZE_MAKE_EXTERNAL_TOAST (10 * 1024 * 1024)

bool pax_enable_debug = true;
bool pax_enable_filter = true;
int pax_scan_reuse_buffer_size = 0;
int pax_max_tuples_per_group = PAX_MAX_TUPLES_PER_GROUP_DEFAULT;
int pax_max_tuples_per_file = PAX_MAX_TUPLES_PER_FILE_DEFAULT;
int pax_max_size_per_file = PAX_MAX_SIZE_PER_FILE_DEFAULT;

bool pax_enable_toast = true;
int pax_min_size_of_compress_toast = PAX_MIN_SIZE_MAKE_COMPRESSED_TOAST;
int pax_min_size_of_external_toast = PAX_MIN_SIZE_MAKE_EXTERNAL_TOAST;

#ifdef ENABLE_PLASMA
bool pax_enable_plasma_in_mem = true;
#endif

}  // namespace pax

namespace paxc {

static bool CheckTuplePerGroup(int *newval, void **extra, GucSource source) {
  bool ok = *newval <= pax::pax_max_tuples_per_file;
  if (!ok) {
    elog(WARNING,
         "The guc pax_max_tuples_per_group should LE with "
         "pax_max_tuples_per_file");
  }
  return ok;
}

static bool CheckTuplePerFile(int *newval, void **extra, GucSource source) {
  bool ok = *newval >= pax::pax_max_tuples_per_group;
  if (!ok) {
    elog(WARNING,
         "The guc pax_max_tuples_per_file should BE with "
         "pax_max_tuples_per_group");
  }
  return ok;
}

static bool CheckMinCompressToastSize(int *newval, void **extra,
                                      GucSource source) {
  bool ok = *newval < pax::pax_min_size_of_external_toast;
  if (!ok) {
    elog(WARNING,
         "The guc pax_min_size_of_compress_toast should LT with "
         "pax_min_size_of_external_toast");
  }
  return ok;
}

static bool CheckMinExternalToastSize(int *newval, void **extra,
                                      GucSource source) {
  bool ok = *newval > pax::pax_min_size_of_compress_toast;
  if (!ok) {
    elog(WARNING,
         "The guc pax_min_size_of_external_toast should BT with "
         "pax_min_size_of_compress_toast");
  }
  return ok;
}

void DefineGUCs() {
  DefineCustomBoolVariable("pax_enable_debug", "enable pax debug", NULL,
                           &pax::pax_enable_debug, true, PGC_USERSET, 0, NULL,
                           NULL, NULL);

  DefineCustomBoolVariable("pax_enable_filter", "enable pax filter", NULL,
                           &pax::pax_enable_filter, true, PGC_USERSET, 0, NULL,
                           NULL, NULL);

#ifdef ENABLE_PLASMA
  DefineCustomBoolVariable("pax_enable_plasma",
                           "Enable plasma cache the set of columns", NULL,
                           &pax::pax_enable_plasma_in_mem, true, PGC_USERSET,
                           GUC_GPDB_NEED_SYNC, NULL, NULL, NULL);
#endif

  DefineCustomIntVariable(
      "pax_scan_reuse_buffer_size", "set the reuse buffer size", NULL,
      &pax::pax_scan_reuse_buffer_size, PAX_SCAN_REUSE_BUFFER_DEFAULT_SIZE,
      PAX_SCAN_REUSE_BUFFER_MIN_SIZE, PAX_SCAN_REUSE_BUFFER_MAX_SIZE,
      PGC_USERSET, GUC_GPDB_NEED_SYNC, NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pax_max_tuples_per_group",
      "the default value for the limit on the number of tuples in a group",
      NULL, &pax::pax_max_tuples_per_group, PAX_MAX_TUPLES_PER_GROUP_DEFAULT,
      PAX_MAX_TUPLES_PER_GROUP_MIN, PAX_MAX_TUPLES_PER_GROUP_MAX, PGC_USERSET,
      0, CheckTuplePerGroup, NULL, NULL);

  DefineCustomIntVariable(
      "pax_max_tuples_per_file",
      "the default value for the limit on the number of tuples in a file", NULL,
      &pax::pax_max_tuples_per_file, PAX_MAX_TUPLES_PER_FILE_DEFAULT,
      PAX_MAX_TUPLES_PER_FILE_MIN, PAX_MAX_NUM_TUPLES_PER_FILE, PGC_USERSET, 0,
      CheckTuplePerFile, NULL, NULL);

  DefineCustomIntVariable(
      "pax_max_size_per_file",
      "the default value for the limit on the number of tuples in a file", NULL,
      &pax::pax_max_size_per_file, PAX_MAX_SIZE_PER_FILE_DEFAULT,
      PAX_MAX_SIZE_PER_FILE_MIN, PAX_MAX_SIZE_PER_FILE_MAX, PGC_USERSET, 0,
      NULL, NULL, NULL);

  DefineCustomBoolVariable("pax_enable_toast", "enable pax toast", NULL,
                           &pax::pax_enable_toast, true, PGC_USERSET, 0, NULL,
                           NULL, NULL);

  DefineCustomIntVariable(
      "pax_min_size_of_compress_toast",
      "the minimum value for creating compress toast", NULL,
      &pax::pax_min_size_of_compress_toast, PAX_MIN_SIZE_MAKE_COMPRESSED_TOAST,
      PAX_MIN_SIZE_MAKE_COMPRESSED_TOAST, PAX_MAX_SIZE_MAKE_COMPRESSED_TOAST,
      PGC_USERSET, 0, CheckMinCompressToastSize, NULL, NULL);

  DefineCustomIntVariable(
      "pax_min_size_of_external_toast",
      "the minimum value for creating external toast", NULL,
      &pax::pax_min_size_of_external_toast, PAX_MIN_SIZE_MAKE_EXTERNAL_TOAST,
      PAX_MIN_SIZE_MAKE_EXTERNAL_TOAST, INT_MAX, PGC_USERSET, 0,
      CheckMinExternalToastSize, NULL, NULL);
}

}  // namespace paxc
