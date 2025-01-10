#ifdef FRONTEND
#include "paxc_desc.h"
#else
#include "storage/wal/paxc_desc.h"
#endif

void pax_rmgr_desc(StringInfo buf, XLogReaderState *record) {
  char *rec = XLogRecGetData(record);
  uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

  switch (info) {
    case XLOG_PAX_INSERT: {
      char filename[MAX_PATH_FILE_NAME_LEN];

      char *rec = XLogRecGetData(record);
      xl_pax_insert *xlrec = (xl_pax_insert *)rec;

      Assert(xlrec->target.file_name_len < MAX_PATH_FILE_NAME_LEN);

      memcpy(filename, rec + SizeOfPAXInsert, xlrec->target.file_name_len);
      filename[xlrec->target.file_name_len] = '\0';

      int32 bufferLen = XLogRecGetDataLen(record) - SizeOfPAXInsert -
                        xlrec->target.file_name_len;
      appendStringInfo(buf,
                       "PAX_INSERT, filename = %s, offset = %ld, "
                       "dataLen = %d",
                       filename, xlrec->target.offset, bufferLen);
      break;
    }
    case XLOG_PAX_CREATE_DIRECTORY: {
      xl_pax_directory *xlrec = (xl_pax_directory *)rec;
      appendStringInfo(buf,
                       "PAX_CREATE_DIRECTORY, dbid = %u, spcId = %u, "
                       "relfilenodeid = %lu",
                       xlrec->node.dbNode, xlrec->node.spcNode,
                       xlrec->node.relNode);
      break;
    }
    case XLOG_PAX_TRUNCATE: {
      xl_pax_directory *xlrec = (xl_pax_directory *)rec;
      appendStringInfo(buf,
                       "PAX_TRUNCATE, dbid = %u, spcId = %u, "
                       "relfilenodeid = %lu",
                       xlrec->node.dbNode, xlrec->node.spcNode,
                       xlrec->node.relNode);
      break;
    }
    default:
      appendStringInfo(buf, "PAX_UNKNOWN: %u", info);
  }
}
const char *pax_rmgr_identify(uint8 info) {
  const char *id = NULL;

  switch (info & ~XLR_INFO_MASK) {
    case XLOG_PAX_INSERT:
      id = "PAX_INSERT";
      break;
    case XLOG_PAX_CREATE_DIRECTORY:
      id = "PAX_CREATE_DIRECTORY";
      break;
    case XLOG_PAX_TRUNCATE:
      id = "PAX_TRUNCATE";
      break;
    default:
      id = "PAX_UNKNOWN";
  }
  return id;
}
