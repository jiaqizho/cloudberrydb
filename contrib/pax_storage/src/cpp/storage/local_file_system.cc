#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pax_access_handle.h"
#include "exceptions/CException.h"

namespace pax {

LocalFile::LocalFile(int fd, const std::string &file_path)
    : File(), fd_(fd), file_path_(file_path) {
  Assert(fd >= 0);
}

ssize_t LocalFile::Read(void *ptr, size_t n) {
  ssize_t num;

  do {
    num = read(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::Write(const void *ptr, size_t n) {
  ssize_t num;

  do {
    num = write(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::PRead(void *ptr, size_t n, off_t offset) {
  ssize_t num;

  do {
    num = pread(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t num;

  do {
    num = pwrite(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

void LocalFile::Close() {
  int rc;

  do {
    rc = close(fd_);
  } while (unlikely(rc == -1 && errno == EINTR));
  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError);
}

size_t LocalFile::FileLength() const {
  struct stat file_stat {};

  CBDB_CHECK(fstat(fd_, &file_stat) == 0,
             cbdb::CException::ExType::kExTypeIOError);
  return static_cast<size_t>(file_stat.st_size);
}

void LocalFile::Flush() {
  CBDB_CHECK(fsync(fd_) == 0, cbdb::CException::ExType::kExTypeIOError);
}

std::string LocalFile::GetPath() const { return file_path_; }

File *LocalFileSystem::Open(const std::string &file_path) {
  LocalFile *local_file;
  int fd = open(file_path.c_str(), O_CREAT | O_RDWR, 0644);

  CBDB_CHECK(fd >= 0, cbdb::CException::ExType::kExTypeIOError);
  local_file = new LocalFile(fd, file_path);
  return local_file;
}

void LocalFileSystem::Delete(const std::string &file_path) const {
  int rc;

  rc = remove(file_path.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError);
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

std::vector<std::string> LocalFileSystem::ListDirectory(
    const std::string &path) const {
  DIR *dir;
  std::vector<std::string> filelist;
  const char *filepath = path.c_str();

  Assert(filepath != NULL && filepath[0] != '\0');

  dir = opendir(filepath);
  CBDB_CHECK(dir, cbdb::CException::ExType::kExTypeFileOperationError);

  try {
    struct dirent *direntry;
    while ((direntry = readdir(dir)) != NULL) {
      char *filename = &direntry->d_name[0];
      // skip to add '.' or '..' direntry for file enumerating under folder on
      // linux OS.
      if (*filename == '.' &&
          (!strcmp(filename, ".") || !strcmp(filename, "..")))
        continue;
      filelist.push_back(std::string(filename));
    }
  } catch (std::exception &ex) {
    closedir(dir);
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError);
  }

  return filelist;
}

void LocalFileSystem::CopyFile(const std::string &src_file_path,
                               const std::string &dst_file_path) const {
  cbdb::CopyFile(src_file_path.c_str(), dst_file_path.c_str());
}

void LocalFileSystem::CreateDirectory(const std::string &path) const {
  cbdb::PathNameCreateDir(path.c_str());
}

void LocalFileSystem::DeleteDirectory(const std::string &path,
                                      bool delete_topleveldir) const {
  cbdb::PathNameDeleteDir(path.c_str(), delete_topleveldir);
}

std::string LocalFileSystem::BuildPaxDirectoryPath(RelFileNode rd_node,
                                                   BackendId rd_backend) const {
  std::string dirpath;
  char *path = cbdb::BuildPaxDirectoryPath(rd_node, rd_backend);
  dirpath = std::string(path);
  cbdb::Pfree(path);
  return dirpath;
}

std::string LocalFileSystem::BuildPaxFilePath(
    const Relation rel, const std::string &block_id) const {
  std::string filepath;
  char *path = cbdb::BuildPaxFilePath(rel, block_id);
  filepath = std::string(path);
  cbdb::Pfree(path);
  return filepath;
}
}  // namespace pax

namespace paxc {
static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel);

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel);

// MakedirRecursive: function used to create directory recursively by a
// specified directory path. parameter path IN directory path. return void.
void MakedirRecursive(const char *path) {
  char dirpath[PAX_MICROPARTITION_NAME_LENGTH];
  unsigned int pathlen = strlen(path);
  struct stat st {};

  Assert(path != NULL && path[0] != '\0' &&
         pathlen < PAX_MICROPARTITION_NAME_LENGTH);

  for (unsigned int i = 0; i <= pathlen; i++) {
    if (path[i] == '/' || path[i] == '\0') {
      strncpy(dirpath, path, i + 1);
      dirpath[i + 1] = '\0';
      if (stat(dirpath, &st) != 0) {
        if (MakePGDirectory(dirpath) != 0)
          ereport(
              ERROR,
              (errcode_for_file_access(),
               errmsg("MakedirRecursive could not create directory \"%s\": %m",
                      dirpath)));
      }
    }
  }
}

// CopyFile: function used to copy all files from specified directory path to
// another specified directory. parameter srcsegpath IN source directory path.
// parameter dstsegpath IN destination directory path.
// parameter dst IN destination relfilenode information.
// return void.
void CopyFile(const char *srcsegpath, const char *dstsegpath) {
  char *buffer = NULL;
  int64 left;
  off_t offset;
  int dstflags;
  //  Note: here File type is defined in PG instead of pax::File class.
  ::File srcfile;
  ::File dstfile;

  Assert(srcsegpath != NULL && srcsegpath[0] != '\0');
  Assert(dstsegpath != NULL && dstsegpath[0] != '\0');

  // TODO(Tony): needs to adjust BLCKSZ for pax storage.
  buffer = reinterpret_cast<char *>(palloc0(BLCKSZ));

  // FIXME(Tony): need to verify if there exits fd leakage problem here.
  srcfile = PathNameOpenFile(srcsegpath, O_RDONLY | PG_BINARY);
  if (srcfile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not open file %s: %m", srcsegpath)));

  // TODO(Tony): need to understand if O_DIRECT flag could be optimzed for data
  // copying in PAX.
  dstflags = O_CREAT | O_WRONLY | O_EXCL | PG_BINARY;

  dstfile = PathNameOpenFile(dstsegpath, dstflags);
  if (dstfile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not create destination file %s: %m",
                           dstsegpath)));

  // TODO(Tony): here needs to implement exception handling for pg function call
  // such as FileDiskSize failure.
  left = FileDiskSize(srcfile);
  if (left < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not seek to end of file %s: %m",
                           srcsegpath)));

  offset = 0;
  while (left > 0) {
    int len;
    CHECK_FOR_INTERRUPTS();
    len = Min(left, BLCKSZ);
    if (FileRead(srcfile, buffer, len, offset, WAIT_EVENT_DATA_FILE_READ) !=
        len)
      ereport(ERROR,
              (errcode_for_file_access(),
               errmsg("CopyFile could not read %d bytes from file \"%s\": %m",
                      len, srcsegpath)));

    if (FileWrite(dstfile, buffer, len, offset, WAIT_EVENT_DATA_FILE_WRITE) !=
        len)
      ereport(ERROR,
              (errcode_for_file_access(),
               errmsg("CopyFile could not write %d bytes to file \"%s\": %m",
                      len, dstsegpath)));

    offset += len;
    left -= len;
  }

  if (FileSync(dstfile, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) != 0)
    ereport(ERROR,
            (errcode_for_file_access(),
             errmsg("CopyFile could not fsync file \"%s\": %m", dstsegpath)));
  FileClose(srcfile);
  FileClose(dstfile);
  pfree(buffer);
}

// DeletePaxDirectoryPath: Delete a directory and everything in it, if it
// exists. parameter dirname IN directory to delete recursively. parameter
// reserve_topdir IN flag indicate if reserve top level directory.
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir) {
  struct stat statbuf {};

  if (stat(dirname, &statbuf) != 0) {
    // Silently ignore missing directory.
    if (errno == ENOENT)
      return;
    else
      ereport(
          ERROR,
          (errcode_for_file_access(),
           errmsg("Check PAX file directory failed, directory path: \"%s\": %m",
                  dirname)));
  }

  DeletePaxDirectoryPathRecursive(dirname, dirname, delete_topleveldir,
                                  UnlinkIfExistsFname, false, LOG);
}

// BuildPaxDirectoryPath: function used to build pax storage directory path
// following pg convension, for example base/{database_oid}/{blocks_relid}_pax.
// parameter rd_node IN relfilenode information.
// parameter rd_backend IN backend transaction id.
// return palloc'd pax storage directory path.
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) {
  char *relpath = NULL;
  char *paxrelpath = NULL;
  relpath = relpathbackend(rd_node, rd_backend, MAIN_FORKNUM);
  Assert(relpath[0] != '\0');
  paxrelpath = psprintf("%s%s", relpath, PAX_MICROPARTITION_DIR_POSTFIX);
  pfree(relpath);
  return paxrelpath;
}

// BuildPaxFilePath: function used to build pax storage directory path following
// pg convension, for example base/{database_oid}/{blocks_relid}_pax. parameter
// rel IN Relation information. parameter block_id IN micro-partition block id.
// return palloc'd pax storage directory path.
char *BuildPaxFilePath(Relation rel, const char *block_id) {
  char *relpath = NULL;
  char *filepath = NULL;

  relpath = BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
  Assert(relpath[0] != '\0');
  filepath = psprintf("%s/%s", relpath, block_id);
  pfree(relpath);
  return filepath;
}

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel) {
  if (isdir) {
    if (rmdir(fname) != 0 && errno != ENOENT)
      ereport(elevel, (errcode_for_file_access(),
                       errmsg("could not remove directory \"%s\": %m", fname)));
  } else {
    // Use PathNameDeleteTemporaryFile to report filesize
    PathNameDeleteTemporaryFile(fname, false);
  }
}

static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel) {
  DIR *dir;
  struct dirent *de;
  dir = AllocateDir(path);

  while ((de = ReadDirExtended(dir, path, elevel)) != NULL) {
    char subpath[MAXPGPATH * 2];
    CHECK_FOR_INTERRUPTS();

    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    snprintf(subpath, sizeof(subpath), "%s/%s", path, de->d_name);

    switch (get_dirent_type(subpath, de, process_symlinks, elevel)) {
      case PGFILETYPE_REG:
        (*action)(subpath, false, elevel);
        break;
      case PGFILETYPE_DIR:
        DeletePaxDirectoryPathRecursive(
            subpath, toplevel_path, delete_topleveldir, action, false, elevel);
        break;
      default:
        break;
    }
  }

  // ignore any error here for delete
  FreeDir(dir);

  // skip deleting top level dir if delete_topleveldir is set to false.
  if (delete_topleveldir || strncmp(path, toplevel_path, strlen(path)) != 0) {
    //  it's important to fsync the destination directory itself as individual
    //  file fsyncs don't guarantee that the directory entry for the file is
    //  synced.  However, skip this if AllocateDir failed; the action function
    //  might not be robust against that.

    if (dir) (*action)(path, true, elevel);
  }
}
}  // namespace paxc
