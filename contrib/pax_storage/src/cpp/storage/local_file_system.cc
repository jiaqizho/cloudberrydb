#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pax {

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

// TODO(gongxun): add error handling
size_t LocalFile::FileLength() const {
  struct stat fileStat;
  if (fstat(fd_, &fileStat) == -1) {
    throw "can't stat file:" + file_path_;
  }
  return static_cast<uint64_t>(fileStat.st_size);
}

void LocalFile::Flush() { fsync(fd_); }

const std::string &LocalFile::GetPath() const { return file_path_; }

File *LocalFileSystem::Open(const std::string &file_path) {
  LocalFile *local_file;
  int fd_ = open(file_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd_ < 0) {
    // TBD: throw here with errno
    return nullptr;
  }
  local_file = new LocalFile(fd_, file_path);
  return local_file;
}

void LocalFileSystem::Delete(const std::string &file_path) const {
  if (remove(file_path.c_str()) != 0) {
    // TBD: throw here with errno
  }
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

}  // namespace pax
