#pragma once

#include "comm/cbdb_api.h"

namespace paxc {
// pax file operation, will refactor it later
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir);
void MakedirRecursive(const char *path);
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend,
                            bool is_dfs_path);
bool PGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft, Oid *oprright,
                     FmgrInfo *finfo);
bool PGGetOperator(const char *operatorName, Oid operatorNamespace,
                   Oid leftObjectId, Oid rightObjectId, Oid *opno,
                   FmgrInfo *finfo);
bool PGGetAddOperator(Oid atttypid, Oid subtype, Oid namespc, Oid *resulttype,
                      FmgrInfo *finfo);
bool PGGetProc(Oid procoid, FmgrInfo *finfo);

bool PGGetAggInfo(const char *procedure, Oid atttypid, Oid *prorettype,
                  Oid *transtype, FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                  bool *final_func_exist, bool *agginitval_isnull);
bool SumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                       FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                       bool *final_func_exist, FmgrInfo *add_finfo);
Datum SumFuncCall(FmgrInfo *flinfo, AggState *state, Datum arg1, Datum arg2);
bool IsDfsTablespaceById(Oid spcId);

bool NeedWAL(Relation rel);

typedef struct PaxFileNodePendingDelete {
  char relkind;
  char *relativePath;
} PaxFileNodePendingDelete;

typedef struct PendingRelDeletePaxFile {
  PendingRelDelete reldelete;        /* base pending delete */
  PaxFileNodePendingDelete filenode; /* relation that may need to be
                                      * deleted */
} PendingRelDeletePaxFile;

void PaxAddPendingDelete(Relation rel, RelFileNode rn_node, bool atCommit);
SMgrRelation PaxRelationCreateStorage(RelFileNode rnode, Relation rel);


}  // namespace paxc
