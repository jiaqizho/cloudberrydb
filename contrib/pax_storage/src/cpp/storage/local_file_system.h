#pragma once

#include <string>
#include <utility>
#include <vector>
#include <sys/stat.h>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"
#include "utils/wait_event.h"

#define PAX_MICROPARTITION_NAME_LENGTH 2048
#define PAX_MICROPARTITION_DIR_POSTFIX "_pax"

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
  std::vector<std::string> ListDirectory(const std::string &path) const override;
  void CopyFile(const std::string &src_file_path, const std::string &dst_file_path) const override;
  void CreateDirectory(const std::string &path) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir) const override;
  std::string BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) const override;
  std::string BuildPaxFilePath(const Relation rel, const std::string &block_id) const override;

 private:
  LocalFileSystem() = default;
};
}  // namespace pax

namespace paxc {
void CopyFile(const char *srcsegpath, const char *dstsegpath);
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir);
void MakedirRecursive(const char *path);
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);
char *BuildPaxFilePath(const Relation rel, const char *block_id);
};  // namespace paxc
