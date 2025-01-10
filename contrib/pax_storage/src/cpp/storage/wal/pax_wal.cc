
#include "storage/wal/pax_wal.h"

#include "comm/cbdb_wrappers.h"
#include "storage/wal/paxc_wal.h"

namespace cbdb {

void XLogPaxInsert(RelFileNode node, const char *filename, int64 offset,
                   void *buffer, int32 bufferLen) {
  CBDB_WRAP_START;
  { paxc::XLogPaxInsert(node, filename, offset, buffer, bufferLen); }
  CBDB_WRAP_END;
}

void XLogPaxCreateDirectory(RelFileNode node) {
  CBDB_WRAP_START;
  { paxc::XLogPaxCreateDirectory(node); }
  CBDB_WRAP_END;
}

void XLogPaxTruncate(RelFileNode node) {
  CBDB_WRAP_START;
  { paxc::XLogPaxTruncate(node); }
  CBDB_WRAP_END;
}

}  // namespace cbdb