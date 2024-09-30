#include "storage/remote_file_system.h"

#include "comm/fmt.h"
#include "comm/pax_resource.h"
#include "exceptions/CException.h"

namespace pax {

class RemoteFile final : public File {
 public:
  RemoteFile(UFile *file, Oid tbl_space_id,
             const std::string &file_path);

  ssize_t Read(void *ptr, size_t n) const override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, off_t offset) override;
  ssize_t PRead(void *ptr, size_t n, off_t offset) const override;
  size_t FileLength() const override;
  void Flush() override;
  void Delete() override;
  void Close() override;
  std::string GetPath() const override;
  std::string DebugString() const override;

 private:
  // UFile is allocated by palloc, MUST use raw pointer here.
  UFile *ufile_;
  Oid tbl_space_id_;
  std::string file_path_;
};

RemoteFile::RemoteFile(UFile *file, Oid tbl_space_id,
                       const std::string &file_path)
    : ufile_(file),
      tbl_space_id_(tbl_space_id),
      file_path_(file_path) {}

ssize_t RemoteFile::Read(void *ptr, size_t n) const {
  ssize_t ret;
  ret = cbdb::UFileRead(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to read [require=%lu, rc=%ld], %s, %s", n, ret,
                 DebugString().c_str(), UFileGetLastError(ufile_)));
  return ret;
}

ssize_t RemoteFile::PRead(void *ptr, size_t n, off_t offset) const {
  ssize_t ret;
  ret = cbdb::UFilePRead(ufile_, (char *)ptr, n, offset);
  CBDB_CHECK(
      ret >= 0, cbdb::CException::kExTypeIOError,
      fmt("Fail to pread [offset=%ld, require=%lu, rc=%ld], %s, %s", offset, n,
          ret, DebugString().c_str(), UFileGetLastError(ufile_)));
  return ret;
}

ssize_t RemoteFile::Write(const void *ptr, size_t n) {
  ssize_t ret;
  ret = cbdb::UFileWrite(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to write [require=%lu, rc=%ld], %s, %s", n, ret,
                 DebugString().c_str(), UFileGetLastError(ufile_)));
  return ret;
}
ssize_t RemoteFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t ret;
  ret = cbdb::UFileWrite(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to pwrite [offset=%ld, len=%ld rc=%ld], %s, %s", offset,
                 n, ret, DebugString().c_str(), UFileGetLastError(ufile_)));
  return ret;
}

size_t RemoteFile::FileLength() const {
  int64_t ret;
  ret = cbdb::UFileSize(ufile_);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to get file size [rc=%ld], %s, %s", ret,
                 DebugString().c_str(), UFileGetLastError(ufile_)));
  return ret;
}

void RemoteFile::Flush() {
  // httpfs (over s3/oss) does not support flush operations.
  // There are bugs in the implementation of UFileSync in the remote file
  // scenario. For the flush semantics of filesystem, it can be called multiple
  // times. However, in the previous implementation of UFileSync, the file fd
  // was closed after the call, and multiple calls would cause exceptions. So
  // for remote files, flush we do nothing.
}

void RemoteFile::Delete() {
  int ret;
  ret = cbdb::UFileUnlink(tbl_space_id_, file_path_.c_str());
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to delete [path=%s, rc=%d], %s, %s", file_path_.c_str(),
                 ret, DebugString().c_str(), UFileGetLastError(ufile_)));
  ufile_ = nullptr;
}

void RemoteFile::Close() {
  int ret;
  const char *err_msg = "";

  Assert(ufile_);
  ret = cbdb::UFileClose(ufile_);
  if (ret < 0) {
    err_msg = UFileGetLastError(ufile_);
  }
  pfree(ufile_);
  ufile_ = nullptr;
  
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             fmt("Fail to delete [rc=%d], %s, %s", ret, DebugString().c_str(),
                 err_msg));
}

std::string RemoteFile::GetPath() const { return file_path_; }

std::string RemoteFile::DebugString() const {
  return fmt("REMOTE file [path=%s, table space id=%d]", file_path_.c_str(),
             tbl_space_id_);
}

// RemoteFileSystem

std::shared_ptr<RemoteFileSystemOptions> RemoteFileSystem::CastOptions(
    const std::shared_ptr<FileSystemOptions> &options) const {
  Assert(options);
  auto remote_options = std::dynamic_pointer_cast<RemoteFileSystemOptions>(options);
  CBDB_CHECK(remote_options, cbdb::CException::kExTypeLogicError,
             "open remote file with invalid options");

  return remote_options;
}

std::shared_ptr<File> RemoteFileSystem::Open(const std::string &file_path, int flags,
                             const std::shared_ptr<FileSystemOptions> &options) {
  std::shared_ptr<RemoteFileSystemOptions> remote_options;
  char errorMessage[UFILE_ERROR_SIZE] = {0};
  UFile *file;

  Assert(options);
  remote_options = CastOptions(options);

  CBDB_CHECK(!(flags & O_RDWR), cbdb::CException::kExTypeIOError,
             fmt("remote file not support O_RDWR flag [path=%s, flags=%d, "
                 "tablespace id=%u]",
                 file_path.c_str(), flags, remote_options->tablespace_id_));

  // remove O_EXCL flag, because it is not supported by remote file system.
  flags &= ~O_EXCL;
  file = cbdb::UFileOpen(remote_options->tablespace_id_, file_path.c_str(),
                         flags, errorMessage, sizeof(errorMessage));
  CBDB_CHECK(
      file, cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to open remote file [path=%s, flags=%d, tablespace id=%u]], %s",
          file_path.c_str(), flags, remote_options->tablespace_id_,
          errorMessage));

  return std::make_shared<RemoteFile>(file, remote_options->tablespace_id_, file_path);
}
std::string RemoteFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}
void RemoteFileSystem::Delete(const std::string &file_path,
                              const std::shared_ptr<FileSystemOptions> &options) const {
  auto remote_options = CastOptions(options);
  cbdb::UFileUnlink(remote_options->tablespace_id_, file_path.c_str());
}

// TODO(gongxun): use a copy method that is more suitable for object storage
int RemoteFileSystem::CopyFile(const File *src_file, File *dst_file) {
  const size_t kBufSize = 32 * 1024;
  char buf[kBufSize];
  ssize_t num_write = 0;
  ssize_t num_read = 0;

  while ((num_read = src_file->Read(buf, kBufSize)) > 0) {
    num_write = dst_file->Write(buf, num_read);
    CBDB_CHECK(num_write == num_read, cbdb::CException::kExTypeIOError,
               fmt("Fail to copy REMOTE file from %s to %s. \n"
                   "Write failed [require=%ld, written=%ld]",
                   src_file->DebugString().c_str(),
                   dst_file->DebugString().c_str(), num_read, num_write));
  }
  // no need check num_read >= 0 again
  // already checked in `src_file->Read`

  return 0;
}

std::vector<std::string> RemoteFileSystem::ListDirectory(
    const std::string &path, const std::shared_ptr<FileSystemOptions> &options) const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}

int RemoteFileSystem::CreateDirectory(const std::string &path,
                                      const std::shared_ptr<FileSystemOptions> &options) const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}
void RemoteFileSystem::DeleteDirectory(const std::string &path,
                                       bool delete_topleveldir,
                                       const std::shared_ptr<FileSystemOptions> &options) const {
  auto remote_options = CastOptions(options);
  cbdb::UFileRmdir(remote_options->tablespace_id_, path.c_str());
}

}  // namespace pax
