-- Ignore "workfile compresssion is not supported by this build" (see
-- 'zlib' test):
--
-- start_matchignore
-- m/ERROR:  workfile compresssion is not supported by this build/
-- end_matchignore
create schema hashagg_spill;
set search_path to hashagg_spill;

-- start_ignore
create language plpython3u;
-- end_ignore

-- force multistage to increase likelihood of spilling
set optimizer_force_multistage_agg = on;

-- set workfile is created to true if all segment did it.
create or replace function hashagg_spill.is_workfile_created(explain_query text)
returns setof int as
$$
import re
query = "select count(*) as nsegments from gp_segment_configuration where role='p' and content = -1;"
rv = plpy.execute(query)
nsegments = int(rv[0]['nsegments'])
rv = plpy.execute(explain_query)
search_text = 'spilling'
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        p = re.compile('.+\((segment -*\d+).+ Workfile: \((\d+) spilling\)')
        m = p.match(cur_line)
        workfile_created = int(m.group(2))
        cur_row = int(workfile_created == nsegments)
        result.append(cur_row)
return result
$$
language plpython3u;

create table testhagg (i1 int, i2 int, i3 int, i4 int);
insert into testhagg select i,i,i,i from
	(select generate_series(1, nsegments * 30000) as i from
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content = -1) foo) bar;
analyze testhagg;

set statement_mem="1800";
set work_mem='1800';
set gp_resqueue_print_operator_memory_limits=on;

-- the number of rows returned by the query varies depending on the number of segments, so
-- only print the first 10
select * from (select max(i1) from testhagg group by i2) foo order by 1 limit 10;
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) select max(i1) from testhagg group by i2;');
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) select max(i1) from testhagg group by i2 limit 90000;');

reset all;
set search_path to hashagg_spill;

-- Test agg spilling scenarios
create table aggspill (i int, j int, t text);
insert into aggspill select i, i*2, i::text from generate_series(1, 10000) i;
analyze aggspill;
insert into aggspill select i, i*2, i::text from generate_series(1, 100000) i;
insert into aggspill select i, i*2, i::text from generate_series(1, 1000000) i;

-- No spill with large statement memory 
set statement_mem = '125MB';
set work_mem='125MB';
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 1) g;

-- Reduce the statement memory to induce spilling
set statement_mem = '10MB';
set work_mem='10MB';
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose)
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 2) g');
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 2) g;

reset optimizer_force_multistage_agg;

-- Test the spilling of aggstates
--     with and without serial/deserial functions
--     with and without workfile compression
-- The transition type of numeric is internal, and hence it uses the serial/deserial functions when spilling
-- The transition type value of integer is by Ref, and it does not have any serial/deserial function when spilling
CREATE TABLE hashagg_spill(col1 numeric, col2 int);
INSERT INTO hashagg_spill SELECT id, 1 FROM generate_series(1,20000) id;
ANALYZE hashagg_spill;
set hash_mem_multiplier = 1;
SET statement_mem='1000kB';
SET gp_workfile_compression = OFF;
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;');
SET gp_workfile_compression = ON;
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;');

-- check spilling to a temp tablespace
CREATE TABLE spill_temptblspace (a numeric);
SET temp_tablespaces=pg_default;
INSERT INTO spill_temptblspace SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;
reset hash_mem_multiplier;
RESET temp_tablespaces;
RESET statement_mem;
RESET gp_workfile_compression;


drop schema hashagg_spill cascade;
