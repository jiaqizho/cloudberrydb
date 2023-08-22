#include "postgres.h"  // NOLINT

#include <stdio.h>

#define USE_PAX_MACRO

#if defined(USE_PAX_MACRO)
/* define these values in pax header file */
#include "comm/cbdb_api.h"
#else
// only for tests, you should use the macros in cbdb_api.h

#define PAX_TABLE_AM_OID 7014
#define PAX_AMNAME "pax"
#define PAX_AM_HANDLER_OID 7600
#define PAX_AM_HANDLER_NAME "pax_tableam_handler"

#define PAX_AUX_STATS_IN_OID 7601
#define PAX_AUX_STATS_OUT_OID 7602
#define PAX_AUX_STATS_TYPE_OID 7603
#define PAX_AUX_STATS_TYPE_NAME "paxauxstats"
#endif

#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#ifdef printf
#undef printf
#endif

#define PAX_COMMENT "column-optimized PAX table access method handler"
int main() {
  printf("-- insert pax catalog values\n");
  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AM_HANDLER_OID,    /* oid: pg_proc.oid */
      PAX_AM_HANDLER_NAME,   /* proname */
      PG_CATALOG_NAMESPACE,  /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      's',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      TABLE_AM_HANDLEROID, /* prorettype: pg_type.oid */
      INTERNALOID,         /* proargtypes: pg_type.oid, internal */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      PAX_AM_HANDLER_NAME, /* prosrc */
      "$libdir/pax",       /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf("INSERT INTO pg_am   VALUES(%u,'%s',%u,'%c');\n",
         PAX_TABLE_AM_OID,   /* pg_am.oid */
         PAX_AMNAME,         /* pg_am.amname */
         PAX_AM_HANDLER_OID, /* pg_am.amhandler: pg_proc.oid */
         't' /* pg_am.amtype: TABLE */);

  printf("COMMENT ON FUNCTION %s IS '%s';\n", PAX_AM_HANDLER_NAME, PAX_COMMENT);

  /* create type for micropartition stats */
  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AUX_STATS_IN_OID,  /* oid: pg_proc.oid */
      "paxauxstats_in",      /* proname */
      PG_CATALOG_NAMESPACE,  /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      'i',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      PAX_AUX_STATS_TYPE_OID, /* prorettype: pg_type.oid */
      CSTRINGOID,             /* proargtypes: pg_type.oid, internal */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      "MicroPartitionStatsInput", /* prosrc */
      "$libdir/pax",              /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AUX_STATS_OUT_OID, /* oid: pg_proc.oid */
      "paxauxstats_out",     /* proname */
      PG_CATALOG_NAMESPACE,  /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      'i',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      CSTRINGOID,             /* proargtypes: pg_type.oid, internal */
      PAX_AUX_STATS_TYPE_OID, /* prorettype: pg_type.oid */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      "MicroPartitionStatsOutput", /* prosrc */
      "$libdir/pax",               /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf(
      "INSERT INTO pg_type "
      "VALUES(%u,'%s',%u,%u,%d,'%c','%c','%c','%c','%c','%c',"
      "%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c',"
      "%u,%d,%d,%u,null,null,null);\n",
      PAX_AUX_STATS_TYPE_OID,  /* pg_type.oid */
      PAX_AUX_STATS_TYPE_NAME, /* pg_type.typname */
      PG_CATALOG_NAMESPACE,    /* pg_type.typnamespace: pg_namespace.oid:
                                  pg_catalog */
      BOOTSTRAP_SUPERUSERID,   /* pg_type.typowner: pg_authid.oid */
      -1,                      /* pg_type.typlen: -1 variable length */
      'f',                     /* pg_type.typbyval */
      'b',                     /* pg_type.typtype */
      'U',                     /* pg_type.typcategory */
      'f',                     /* pg_type.typispreferred */
      't',                     /* pg_type.typisdefined */
      ',',                     /* pg_type.typdelim */
      InvalidOid,              /* pg_type.typrelid */
      InvalidOid,              /* pg_type.typsubscript */
      InvalidOid,              /* pg_type.typelem */
      InvalidOid,              /* pg_type.typarray */
      PAX_AUX_STATS_IN_OID,    /* pg_type.typinput */
      PAX_AUX_STATS_OUT_OID,   /* pg_type.typoutput */
      InvalidOid,              /* pg_type.typreceive */
      InvalidOid,              /* pg_type.typsend */
      InvalidOid,              /* pg_type.typmodin */
      InvalidOid,              /* pg_type.typmodout */
      InvalidOid,              /* pg_type.typanalyze */
      'i',                     /* pg_type.typalign */
      'x',                     /* pg_type.typstorage */
      't',                     /* pg_type.typnotnull */
      InvalidOid,              /* pg_type.typbasetype */
      -1,                      /* pg_type.typtypmod */
      0,                       /* pg_type.ndims */
      InvalidOid               /* pg_type.typcollation */
  );

  return 0;
}
