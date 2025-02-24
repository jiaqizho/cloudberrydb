#pragma once

namespace pax {
extern bool pax_enable_debug;
extern bool pax_enable_sparse_filter;
extern bool pax_enable_row_filter;
extern int pax_scan_reuse_buffer_size;
extern int pax_max_tuples_per_group;

extern int pax_max_tuples_per_file;
extern int pax_max_size_per_file;

extern bool pax_enable_toast;
extern int pax_min_size_of_compress_toast;
extern int pax_min_size_of_external_toast;

extern char *pax_default_storage_format;
extern int pax_bloom_filter_work_memory_bytes;
extern bool pax_log_filter_tree;
}  // namespace pax

namespace paxc {
extern void DefineGUCs();
}
