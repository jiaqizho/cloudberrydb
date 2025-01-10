#pragma once

#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"
extern "C" {
#endif
#include "c.h"

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

// FIXME(gongxun):
// there are three types of pax file, data file, toast file and visimap file,
// 1. data file: the filename is the block id
// 2. toast file: the filename is the block id + ".toast"
// 3. visimap file: the filename is the block id + ".visimap"
// so the max length of the filename should never be larger than 64
#define MAX_PATH_FILE_NAME_LEN 64

// should different from the rmgr id of the other storage engine
// https://wiki.postgresql.org/wiki/CustomWALResourceManagers
#define PAX_RMGR_ID 199

#define XLOG_PAX_INSERT 0x00
#define XLOG_PAX_CREATE_DIRECTORY 0x10
#define XLOG_PAX_TRUNCATE 0x20

typedef struct xl_pax_target {
  RelFileNode node;
  uint16 file_name_len;
  int64 offset;
} xl_pax_target;

#define SizeOfPAXTarget (sizeof(xl_pax_target))

/**
 * layout of the pax insert:
 *
 * +-----------------+
 * |  RelFileNode    |
 * +-----------------+
 * |  file_name_len  |
 * +-----------------+
 * |  offset         |
 * +-----------------+
 * |  file_name      |
 * +-----------------+
 * |  data           |
 * +-----------------+
 */
typedef struct xl_pax_insert {
  // meta data about the inserted block of pax data
  xl_pax_target target;
  // BLOCK DATA FOLLOWS AT END OF STRUCT
} xl_pax_insert;

#define SizeOfPAXInsert (sizeof(xl_pax_insert))

/**
 * layout of the pax directory:
 *
 * +-----------------+
 * |  RelFileNode    |
 * +-----------------+
 */
typedef struct xl_pax_directory {
  RelFileNode node;
} xl_pax_directory;

#define SizeOfPAXDirectory sizeof(xl_pax_directory)

extern void pax_rmgr_desc(StringInfo buf, XLogReaderState *record);
extern const char *pax_rmgr_identify(uint8 info);
#ifdef __cplusplus
};
#pragma GCC diagnostic pop
#endif
