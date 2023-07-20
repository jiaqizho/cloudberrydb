#include "postgres.h" // NOLINT

#include <stdio.h>

#define USE_PAX_MACRO

#if defined(USE_PAX_MACRO)
/* define these values in pax header file */
#include "comm/cbdb_api.h"
#else
#define PAX_TABLE_AM_OID (BITMAP_AM_OID + 1)
#define PAX_AMNAME "pax"
#define PAX_AM_HANDLER_OID 7600
#define PAX_AM_HANDLER_NAME "pax_tableam_handler"
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
      'a'  /* proexeclocation: all */);

  printf("INSERT INTO pg_am   VALUES(%u,'%s',%u,'%c');\n",
         PAX_TABLE_AM_OID,   /* pg_am.oid */
         PAX_AMNAME,         /* pg_am.amname */
         PAX_AM_HANDLER_OID, /* pg_am.amhandler: pg_proc.oid */
         't' /* pg_am.amtype: TABLE */);

  printf("COMMENT ON FUNCTION %s IS '%s';\n",
         PAX_AM_HANDLER_NAME, PAX_COMMENT);

  return 0;
}
