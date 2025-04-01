-- @Description Tests the compaction of data inserted in utility mode
-- 
DROP TABLE IF EXISTS foo;
CREATE TABLE foo (a INT, b INT, c CHAR(128)) distributed randomly;
CREATE INDEX foo_index ON foo(b);

0U: INSERT INTO foo VALUES (2, 2, 'c');
0U: INSERT INTO foo VALUES (3, 3, 'c');
SELECT segment_id, pttupcount FROM get_pax_aux_table_all('foo');
DELETE FROM foo WHERE a = 2;
UPDATE foo SET b = -1 WHERE a = 3;
VACUUM foo;
SELECT segment_id, pttupcount FROM get_pax_aux_table_all('foo') where pttupcount > 0;
