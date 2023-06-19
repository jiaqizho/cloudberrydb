#pragma once

#include <string>
#include <utility>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {

class LocalFile final : public File {
 public:
  LocalFile(int fd, const std::string &file_path);

  ssize_t Read(void *ptr, size_t n) override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, size_t offset) override;
  ssize_t PRead(void *ptr, size_t n, size_t offset) override;
  size_t FileLength() const override;
  void Close() override;
  void Flush() override;
  const std::string &GetPath() const override;

 private:
  int fd_;
  std::string file_path_;
  // TODO(jiaqizho): added resource owner
};

class LocalFileSystem final : public FileSystem {
  friend class Singleton<LocalFileSystem>;

 public:
  File *Open(const std::string &file_path) override;
  std::string BuildPath(const File *file) const override;
  void Delete(const std::string &file_path) const override;

 private:
  LocalFileSystem() = default;
};

}  // namespace pax
