#pragma once

#include "comm/cbdb_api.h"
#define N_BYTES 8
namespace paxc {

// should match the behavior in the datum_to_bytes function
bool support_zorder_type(Oid type);
};  // namespace paxc

namespace pax {

// Convert several types to byte representations which could be compared
// lexicographically.
void datum_to_bytes(Datum datum, Oid type, bool isnull, char *result);

int bytes_compare(const char *a, const char *b, int ncolumns);

Datum bytes_to_zorder_datum(char *buffer, int ncolumns);

void interleave_bits(const char *src, char *result, int ncolumns);

}  // namespace pax