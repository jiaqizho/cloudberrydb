#include "storage/pax_itemptr.h"

#include "catalog/pax_fastsequence.h"

namespace pax {
std::string GenerateBlockID(Relation relation) {
  int32 seqno = cbdb::CPaxGetFastSequences(RelationGetRelid(relation));
  return std::to_string(seqno);
}
}  // namespace pax
