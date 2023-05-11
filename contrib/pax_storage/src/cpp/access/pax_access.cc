#include "access/pax_access.h"

#include "access/pax_deleter.h"
#include "access/pax_dml_state.h"
#include "access/pax_inserter.h"
#include "access/pax_scanner.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
void CPaxAccess::PaxCreateAuxBlocks(const Relation relation) {
  cbdb::PaxCreateMicroPartitionTable(relation);
}

}  // namespace pax
