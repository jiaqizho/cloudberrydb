#pragma once

#include <sys/stat.h>

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {

class LocalFileSystem final : public FileSystem {
  friend class Singleton<LocalFileSystem>;
  friend class ClassCreator;

 public:
  LocalFileSystem() = default;
  std::shared_ptr<File> Open(const std::string &file_path, int flags,
             const std::shared_ptr<FileSystemOptions> &options = nullptr) override;
  void Delete(const std::string &file_path,
              const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  std::vector<std::string> ListDirectory(
      const std::string &path,
      const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  int CreateDirectory(const std::string &path,
                      const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                       const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  // operate with file
  std::string BuildPath(const File *file) const override;

  int CopyFile(const File *src_file, File *dst_file) override;
};
}  // namespace pax
