#pragma once

#include <string>
#include <vector>
#include "comm/cbdb_wrappers.h"

namespace pax {

class File {
 public:
  virtual ~File() = default;
  virtual ssize_t Read(void *ptr, size_t n) = 0;
  virtual ssize_t Write(const void *ptr, size_t n) = 0;
  virtual ssize_t PWrite(const void *buf, size_t count, size_t offset) = 0;
  virtual ssize_t PRead(void *buf, size_t count, size_t offset) = 0;
  virtual size_t FileLength() const = 0;
  virtual void Flush() = 0;
  virtual void Close() = 0;

  virtual const std::string &GetPath() const = 0;
};

class FileSystem {
 public:
  virtual ~FileSystem() = default;
  virtual File *Open(const std::string &file_path) = 0;
  virtual std::string BuildPath(const File *file) const = 0;
  virtual void Delete(const std::string &file_path) const = 0;
  virtual std::vector<std::string> ListDirectory(const std::string &path) const = 0;
  virtual void CopyFile(const std::string &srcFilePath, const std::string &dstFilePath) const = 0;
  virtual void CreateDirectory(const std::string &path) const = 0;
  virtual void DeleteDirectory(const std::string &path, bool delete_topleveldir) const = 0;
  virtual std::string BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) const = 0;
  virtual std::string BuildPaxFilePath(const Relation rel, const std::string &block_id) const = 0;

 protected:
};

}  //  namespace pax
