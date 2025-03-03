#include "manifest_wrapper.h"
extern "C" {
#include "catalog/storage.h"
}
#include "comm/paxc_wrappers.h"
#include "exceptions/CException.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/wal/paxc_wal.h"

bool paxc_is_dfs(Oid tablespace) {
  return paxc::IsDfsTablespaceById(tablespace);
}

bool paxc_need_wal(Relation rel) {
  return paxc::NeedWAL(rel);
}

char *paxc_get_pax_dir(RelFileNode rnode, BackendId backend, bool is_dfs) {
  return paxc::BuildPaxDirectoryPath(rnode, backend, is_dfs);
}
char *paxc_build_path(const char *filename);

void paxc_create_pax_directory(Relation rel, RelFileNode newrnode, bool is_dfs) {
  // create relfilenode file for pax table
  auto srel = paxc::PaxRelationCreateStorage(newrnode, rel);
  smgrclose(srel);

  if (is_dfs) return;
  
  char *path = paxc::BuildPaxDirectoryPath(newrnode, rel->rd_backend, false);
  Assert(path);

  int rc;
  CBDB_TRY();
  {
    pax::FileSystem *fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    rc = fs->CreateDirectory(path);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  if (rc != 0)
    elog(ERROR, "create data dir failed for %u/%u/%lu",
                newrnode.dbNode, newrnode.spcNode, newrnode.relNode);
}

void paxc_store_file(const char *filename, const void *data, size_t size) {
  CBDB_TRY();
  {
    auto fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    auto file = fs->Open(filename, pax::fs::kWriteMode);
    file->WriteN(data, size);
    file->Close();
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();
}

void paxc_read_all(const char *filename, void (*func)(const void *ptr, size_t size, void *opaque), void *opaque) {
  CBDB_TRY();
  {
    std::vector<uint8> v;
    auto fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    auto file = fs->Open(filename, pax::fs::kReadMode);
    auto filesize = file->FileLength();
    v.resize(filesize);
    file->ReadN(v.data(), filesize);
    file->Close();
    CBDB_WRAP_START;
    {
      func(v.data(), v.size(), opaque);
    }
    CBDB_WRAP_END;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

}

void paxc_wal_insert_if_required(Relation rel, const char *filename, const void *data, size_t size, int64 offset) {
  if (paxc::NeedWAL(rel)) {
    paxc::XLogPaxInsert(rel->rd_node, filename, offset, const_cast<void*>(data), size);
  }
}

void paxc_wal_truncate_directory(RelFileNode node) {
  paxc::XLogPaxTruncate(node);
}