#include "comm/paxc_utils.h"
#include "comm/cbdb_wrappers.h"
#include "utils/wait_event.h"
#include <sys/stat.h>

namespace paxc {
// ListDirectory: function used to list all files under specified directory path.
// parameter path IN directory path.
// return List* all file names under specified directory path.
List* ListDirectory(const char *path) {
  DIR *dir;
  List *fileList = NIL;

  Assert(path != NULL && path[0] != '\0');

  dir = opendir(path);
  if (!dir) {
    ereport(ERROR,
           (errcode_for_file_access(),
           errmsg("ListDirectory could not open directory %s: %m", path)));
    return NIL;
  }

  PG_TRY();
  {
    struct dirent *direntry;
    while ((direntry = readdir(dir)) != NULL) {
      char *filename = &direntry->d_name[0];
      // skip to add '.' or '..' direntry for file enumerating under folder on linux OS.
      if (*filename == '.' && (!strcmp(filename, ".") || !strcmp(filename, "..")))
        continue;
      filename = reinterpret_cast<char *>(pstrdup(filename));
      fileList = lappend(fileList, filename);
    }
  }
  PG_FINALLY();
  {
    closedir(dir);
  }
  PG_END_TRY();

  return fileList;
}

// CopyFile: function used to copy all files from specified directory path to another specified directory.
// parameter srcsegpath IN source directory path.
// parameter dstsegpath IN destination directory path.
// parameter dst IN destination relfilenode information.
// return void.
void CopyFile(const char *srcsegpath, const char *dstsegpath) {
  char *buffer = NULL;
  int64 left;
  off_t offset;
  int dstflags;
  File srcFile;
  File dstFile;

  Assert(srcsegpath != NULL && srcsegpath[0] != '\0');
  Assert(dstsegpath != NULL && dstsegpath[0] != '\0');

  // TODO(Tony): needs to adjust BLCKSZ for pax storage.
  buffer = reinterpret_cast<char *>(palloc0(BLCKSZ));

  // FIXME(Tony): need to verify if there exits fd leakage problem here.
  srcFile = PathNameOpenFile(srcsegpath, O_RDONLY | PG_BINARY);
  if (srcFile < 0)
    ereport(ERROR,
           (errcode_for_file_access(),
           errmsg("CopyFile could not open file %s: %m", srcsegpath)));

  // TODO(Tony): need to understand if O_DIRECT flag could be optimzed for data copying in PAX.
  dstflags = O_CREAT | O_WRONLY | O_EXCL | PG_BINARY;

  dstFile = PathNameOpenFile(dstsegpath, dstflags);
  if (dstFile < 0)
    ereport(ERROR,
           (errcode_for_file_access(),
           errmsg("CopyFile could not create destination file %s: %m", dstsegpath)));

  // TODO(Tony): here needs to implement exception handling for pg function call such as FileDiskSize failure.
  left = FileDiskSize(srcFile);
  if (left < 0)
    ereport(ERROR,
           (errcode_for_file_access(),
           errmsg("CopyFile could not seek to end of file %s: %m", srcsegpath)));

  offset = 0;
  while (left > 0) {
    int len;
    CHECK_FOR_INTERRUPTS();
    len = Min(left, BLCKSZ);
    if (FileRead(srcFile, buffer, len, offset, WAIT_EVENT_DATA_FILE_READ) != len)
      ereport(ERROR,
             (errcode_for_file_access(),
             errmsg("CopyFile could not read %d bytes from file \"%s\": %m",
             len, srcsegpath)));

    if (FileWrite(dstFile, buffer, len, offset, WAIT_EVENT_DATA_FILE_WRITE) != len)
      ereport(ERROR,
             (errcode_for_file_access(),
             errmsg("CopyFile could not write %d bytes to file \"%s\": %m",
             len, dstsegpath)));

    offset += len;
    left -= len;
  }

  if (FileSync(dstFile, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) != 0)
    ereport(ERROR,
           (errcode_for_file_access(),
           errmsg("CopyFile could not fsync file \"%s\": %m",
           dstsegpath)));
  FileClose(srcFile);
  FileClose(dstFile);
  pfree(buffer);
}

// MakedirRecursive: function used to create directory recursively by a specified directory path.
// parameter path IN directory path.
// return void.
void MakedirRecursive(const char *path) {
  char dirpath[PAX_MICROPARTITION_NAME_LENGTH];
  char pathlen = strlen(path);
  struct stat st;

  Assert(path != NULL && path[0] != '\0' &&
         pathlen < PAX_MICROPARTITION_NAME_LENGTH);

  for (int i = 0; i <= pathlen; i++) {
    if (path[i] == '/' || path[i] == '\0') {
      strncpy(dirpath, path, i+1);
      dirpath[i+1] = '\0';
      if (stat(dirpath, &st) != 0) {
        if (MakePGDirectory(dirpath) != 0)
          ereport(ERROR,
                 (errcode_for_file_access(),
                 errmsg("MakedirRecursive could not create directory \"%s\": %m", dirpath)));
      }
    }
  }
}

// BuildPaxDirectoryPath: function used to build pax storage directory path, for example base/13261/16392_pax.
// parameter RelFileNode IN relfilenode information.
// parameter rd_backend IN backend transaction id.
// return palloc'd pax storage directory path.
char* BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) {
  char *relpath = NULL;
  char *paxrelpath = NULL;
  relpath = relpathbackend(rd_node, rd_backend, MAIN_FORKNUM);
  Assert(relpath[0] != '\0');
  paxrelpath = psprintf("%s%s", relpath, PAX_MICROPARTITION_DIR_POSTFIX);
  pfree(relpath);
  return paxrelpath;
}

// CreateMicroPartitionFileDirectory: function used to create directory to store MicroPartition table files.
// parameter rel IN pax table relation information.
// parameter rd_backend IN rd_backend id.
// parameter persistence IN flag to indicate storage persistency.
// return void.
void CreateMicroPartitionFileDirectory(const RelFileNode *rel,
                                       const BackendId rd_backend,
                                       char persistence) {
  char *relpath = NULL;
  SMgrRelation srel;

  // Create pax table micropartition file path following pg convention,
  // for example base/{database_oid}/{blocks_relid}_pax.
  relpath = paxc::BuildPaxDirectoryPath(*rel, rd_backend);
  Assert(relpath[0] != '\0');
  MakePGDirectory(relpath);

  // Create pax table relfilenode file and database directory under path base/,
  // The relfilenode created here is to be compatible with PG normal process logic
  // instead of being used by pax storage.
  srel = RelationCreateStorage(*rel, persistence, SMGR_MD);
  smgrclose(srel);
  pfree(relpath);
}
}  // namespace paxc

