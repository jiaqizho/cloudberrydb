#pragma once

#include "manifest.h"

ManifestTuple manifesttuple_from_values(ManifestDesc tupledescriptor,
                                        const MetaValue data[],
                                        int count /*, bool isnulls[]*/);
void markTupleDelete(ManifestTuple tuple);
ManifestTuple merge_tuple(ManifestTuple tuple, ManifestDesc tupledescriptor,
                          const MetaValue data[], int count);

ManifestTuple make_empty_tuple(ManifestDesc desc);
