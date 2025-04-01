-- start_ignore
create schema pax_test;
CREATE EXTENSION gp_inject_fault;
-- end_ignore

CREATE OR REPLACE FUNCTION pax_get_catalog_rows(rel regclass,
  segment_id out int,
  ptblockname out int,
  pttupcount out int,
  ptblocksize out int,
  ptstatistics out pg_ext_aux.paxauxstats,
  ptvisimapname out name,
  pthastoast out boolean,
  ptisclustered out boolean
)
returns setof record
EXECUTE ON  ALL SEGMENTS
AS '$libdir/pax', 'pax_get_catalog_rows' LANGUAGE C ;

CREATE OR REPLACE FUNCTION get_pax_aux_table(rel regclass)
RETURNS TABLE(
  ptblockname integer,
  pttupcount integer,
  ptstatistics pg_ext_aux.paxauxstats,
  ptexistvisimap bool,
  ptexistexttoast bool,
  ptisclustered bool
) AS $$
  SELECT ptblockname, pttupcount, ptstatistics, ptvisimapname IS NOT NULL, pthastoast, ptisclustered
  FROM pax_get_catalog_rows(rel);
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION get_pax_table_stats(rel regclass)
  RETURNS TABLE("ptstatistics" pg_ext_aux.paxauxstats) AS $$
  SELECT ptstatistics FROM pax_get_catalog_rows(rel);
$$ LANGUAGE sql;
