#include "storage/paxc_smgr.h"

#include "comm/paxc_wrappers.h"

namespace paxc {

static void mdunlink_pax(RelFileNodeBackend rnode, ForkNumber forkNumber,
                         bool isRedo) {
  // remove the data directory of pax table
  // FIXME(gongxun): can work well with dfs_tablespace
  if (forkNumber == MAIN_FORKNUM) {
    const char *path =
        paxc::BuildPaxDirectoryPath(rnode.node, rnode.backend, false);
    paxc::DeletePaxDirectoryPath(path, true);
  }

  const f_smgr *smgr = smgr_get(SMGR_MD);
  Assert(smgr != NULL);
  smgr->smgr_unlink(rnode, forkNumber, isRedo);
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

void RegisterPaxSmgr() { smgr_register(&pax_smgr, SMGR_PAX); }

}  // namespace paxc