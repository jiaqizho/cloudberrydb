#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "exceptions/CException.h"

namespace pax {

LocalFile::LocalFile(int fd, const std::string &file_path)
    : File(), fd_(fd), file_path_(file_path) {
  Assert(fd != -1);
}

ssize_t LocalFile::Read(void *ptr, size_t n) { return read(fd_, ptr, n); }

ssize_t LocalFile::Write(const void *ptr, size_t n) {
  return write(fd_, ptr, n);
}

ssize_t LocalFile::PRead(void *ptr, size_t n, size_t offset) {
  return pread(fd_, ptr, n, offset);
}

ssize_t LocalFile::PWrite(const void *ptr, size_t n, size_t offset) {
  return pwrite(fd_, ptr, n, offset);
}

void LocalFile::Close() { close(fd_); }

size_t LocalFile::FileLength() const {
  struct stat file_stat {};
  if (fstat(fd_, &file_stat) == -1) {
    elog(WARNING, "fstat failed. %m");
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
  }
  return static_cast<uint64>(file_stat.st_size);
}

void LocalFile::Flush() { fsync(fd_); }

const std::string &LocalFile::GetPath() const { return file_path_; }

File *LocalFileSystem::Open(const std::string &file_path) {
  LocalFile *local_file;
  int fd = open(file_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    elog(WARNING, "open failed. %m");
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
  }
  local_file = new LocalFile(fd, file_path);
  return local_file;
}

void LocalFileSystem::Delete(const std::string &file_path) const {
  if (remove(file_path.c_str()) != 0) {
    elog(WARNING, "remove failed. %m");
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
  }
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

}  // namespace pax
