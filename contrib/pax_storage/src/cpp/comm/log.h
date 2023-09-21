#pragma once

// FIXME: reimplement log more robust
#define PAX_LOG_IF(ok, ...) do { if (ok) elog(LOG, __VA_ARGS__); } while(0)
