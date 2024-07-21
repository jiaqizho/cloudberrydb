-- start_ignore
create schema pax_test;
CREATE EXTENSION gp_inject_fault;
-- end_ignore

CREATE OR REPLACE FUNCTION "get_pax_aux_table"(table_name text)
  RETURNS TABLE("ptblockname" name,"pttupcount" integer, 
    "ptstatistics" pg_ext_aux.paxauxstats, 
    "ptexistvisimap" bool, "ptexistexttoast" bool, "ptisclustered" bool) AS $BODY$
    DECLARE
    subquery  varchar;
    pre_sql   varchar;
    table_oid Oid;
    begin
    pre_sql:='select oid from pg_class where relname='''||table_name||'''';
    EXECUTE pre_sql into table_oid;
    subquery := 'select ptblockname, pttupcount, ptstatistics, ptvisimapname IS NOT NULL AS ptexistvisimap, ptexistexttoast, ptisclustered from gp_dist_random(''pg_ext_aux.pg_pax_blocks_'||table_oid||''')';
    RETURN QUERY execute subquery;
    END
    $BODY$
  LANGUAGE plpgsql;
