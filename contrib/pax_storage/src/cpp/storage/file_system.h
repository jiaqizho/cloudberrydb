#pragma once

#include <string>

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

 protected:
};

}  //  namespace pax
