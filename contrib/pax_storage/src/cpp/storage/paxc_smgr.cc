#include "storage/paxc_smgr.h"

#include "comm/paxc_wrappers.h"
#include "storage/paxc_smgr.h"
#include "storage/wal/paxc_wal.h"

#include <unistd.h>

smgr_get_impl_hook_type prev_smgr_get_impl_hook = NULL;
extern smgr_get_impl_hook_type smgr_get_impl_hook;

namespace paxc {

static void mdunlink_pax(RelFileNodeBackend rnode, ForkNumber forkNumber,
                         bool isRedo) {
  // remove the data directory of pax table
  // FIXME(gongxun): can work well with dfs_tablespace
  if (forkNumber == MAIN_FORKNUM) {
    const char *path =
        paxc::BuildPaxDirectoryPath(rnode.node, rnode.backend, false);
    paxc::DeletePaxDirectoryPath(path, true);

    if (isRedo) {
      paxc::XLogForgetRelation(rnode.node);
    }
  }

  // unlink the relfilenode file directly, mdunlink will not remove
  // the relfilenode file, only truncate it if isRedo is false.
  auto relpath = relpath(rnode, MAIN_FORKNUM);
  auto ret = unlink(relpath);
  if (ret == -1 && errno != ENOENT) {
    ereport(WARNING, (errcode_for_file_access(),
                      errmsg("PAX: could not remove file \"%s\": %m", relpath)));
  }
  pfree(relpath);
}

static const f_smgr pax_smgr = {
    .smgr_name = "pax",
    .smgr_init = mdinit,
    .smgr_shutdown = NULL,
    .smgr_open = mdopen,
    .smgr_close = mdclose,
    .smgr_create = mdcreate,
    .smgr_exists = mdexists,
    .smgr_unlink = mdunlink_pax,
    .smgr_extend = mdextend,
    .smgr_prefetch = mdprefetch,
    .smgr_read = mdread,
    .smgr_write = mdwrite,
    .smgr_writeback = mdwriteback,
    .smgr_nblocks = mdnblocks,
    .smgr_truncate = mdtruncate,
    .smgr_immedsync = mdimmedsync,
};

void pax_smgr_get_impl_hook(const Relation rel, SMgrImpl *smgr_impl) {
  Assert(rel != NULL);

  if (RelationIsPAX(rel)) {
    *smgr_impl = SMGR_PAX;
  } else {
    if (prev_smgr_get_impl_hook) {
      prev_smgr_get_impl_hook(rel, smgr_impl);
    }
  }
}

void RegisterPaxSmgr() {
  smgr_register(&pax_smgr, SMGR_PAX);

  prev_smgr_get_impl_hook = smgr_get_impl_hook;
  smgr_get_impl_hook = pax_smgr_get_impl_hook;
}

}  // namespace paxc
