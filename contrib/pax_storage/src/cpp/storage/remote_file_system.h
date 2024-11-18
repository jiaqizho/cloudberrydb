#pragma once

#include "comm/cbdb_api.h"

#include <map>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {
class RemoteFileSystem;

struct RemoteFileSystemOptions final : public FileSystemOptions {
 public:
  RemoteFileSystemOptions() = default;
  RemoteFileSystemOptions(Oid tablespace_id) : tablespace_id_(tablespace_id) {}

  virtual ~RemoteFileSystemOptions() {}
  Oid tablespace_id_;
};

class RemoteFileSystem final : public FileSystem {
  friend class Singleton<RemoteFileSystem>;
  friend class ClassCreator;

 public:
  std::unique_ptr<File> Open(const std::string &file_path, int flags,
             const std::shared_ptr<FileSystemOptions> &options) override;
  void Delete(const std::string &file_path,
              const std::shared_ptr<FileSystemOptions> &options) const override;
  std::vector<std::string> ListDirectory(
      const std::string &path, const std::shared_ptr<FileSystemOptions> &options) const override;
  int CreateDirectory(const std::string &path,
                      const std::shared_ptr<FileSystemOptions> &options) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                       const std::shared_ptr<FileSystemOptions> &options) const override;

  // operate with file
  std::string BuildPath(const File *file) const override;
  int CopyFile(const File *src_file, File *dst_file) override;

 private:
  RemoteFileSystem() = default;

  std::shared_ptr<RemoteFileSystemOptions> CastOptions(const std::shared_ptr<FileSystemOptions> &options) const;
};
}  // namespace pax
