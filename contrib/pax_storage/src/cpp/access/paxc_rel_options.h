#pragma once

#include "comm/cbdb_api.h"

#include "exceptions/CException.h"
#include "storage/proto/proto_wrappers.h"  // for ColumnEncoding_Kind

namespace paxc {

#define ColumnEncoding_Kind_NO_ENCODED_STR "none"
#define ColumnEncoding_Kind_RLE_V2_STR "rle"
#define ColumnEncoding_Kind_DIRECT_DELTA_STR "delta"
#define ColumnEncoding_Kind_COMPRESS_ZSTD_STR "zstd"
#define ColumnEncoding_Kind_COMPRESS_ZLIB_STR "zlib"

typedef struct {
  const char *optname; /* option's name */
  const pax::ColumnEncoding_Kind kind;
} relopt_compress_type_mapping;

static const relopt_compress_type_mapping kSelfRelCompressMap[] = {
    {ColumnEncoding_Kind_NO_ENCODED_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED},
    {ColumnEncoding_Kind_RLE_V2_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2},
    {ColumnEncoding_Kind_DIRECT_DELTA_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA},
    {ColumnEncoding_Kind_COMPRESS_ZSTD_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD},
    {ColumnEncoding_Kind_COMPRESS_ZLIB_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB},
};

#define PAX_DEFAULT_COMPRESSLEVEL AO_DEFAULT_COMPRESSLEVEL
#define PAX_MIN_COMPRESSLEVEL AO_MIN_COMPRESSLEVEL
#define PAX_MAX_COMPRESSLEVEL AO_MAX_COMPRESSLEVEL
#define PAX_DEFAULT_COMPRESSTYPE ColumnEncoding_Kind_NO_ENCODED_STR

#define PAX_SOPT_STORAGE_FORMAT "storage_format"
#define PAX_SOPT_COMPTYPE SOPT_COMPTYPE
#define PAX_SOPT_COMPLEVEL SOPT_COMPLEVEL

// plain structure used by reloptions, can be accessed from C++ code.
struct PaxOptions {
  int32 vl_len; /* varlena header (do not touch directly!) */
  char storage_format[16];
  char compress_type[16];
  int compress_level;
};

/*
 * used to register pax rel options
 */
void paxc_reg_rel_options();

/*
 * parse the rel options in `pg_attribute_encoding` and relation
 * if no ENCODING setting in `pg_attribute_encoding` will fill with
 * the default one
 */
bytea *paxc_default_rel_options(Datum reloptions, char /*relkind*/,
                                bool validate);

/*
 * parse the attr options from `pg_attribute_encoding`
 * if no ENCODING setting in `pg_attribute_encoding` will fill with
 * the default one
 */
PaxOptions **paxc_relation_get_attribute_options(Relation rel);

/*
 * validate the ENCODING CLAUSES
 * like `CREATE TABLE t1 (c1 int, COLUMN c1 ENCODING (key=value)) using
 * pax`
 */
void paxc_validate_column_encoding_clauses(List *encoding_opts);

/*
 * transform the ENCODING options if key no setting
 * validate will become true only when the encoding syntax is true
 * like `CREATE TABLE t1 (c1 int ENCODING (key=value)) using pax`
 *
 * pax no need transform the ENCODING options if key no setting
 * it will deal the default value inside pax colomn
 */
List *paxc_transform_column_encoding_clauses(List *encoding_opts, bool validate,
                                             bool fromType);

}  // namespace paxc

namespace pax {

// use to transform compress type str to encoding kind
static inline ColumnEncoding_Kind CompressKeyToColumnEncodingKind(
    const char *encoding_str) {
  Assert(encoding_str);

  for (size_t i = 0; i < lengthof(paxc::kSelfRelCompressMap); i++) {
    if (encoding_str &&
        strcmp(paxc::kSelfRelCompressMap[i].optname, encoding_str) == 0) {
      return paxc::kSelfRelCompressMap[i].kind;
    }
  }

  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}
}  // namespace pax
