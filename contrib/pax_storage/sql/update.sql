set default_table_access_method = pax;
set pax.enable_filter = off;

--
-- UPDATE ... SET <col> = DEFAULT;
--

CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
);

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);

SELECT a,b,c FROM update_test ORDER BY a,b,c;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT a,b,c FROM update_test ORDER BY a,b,c;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT a,b,c FROM update_test ORDER BY a,b,c;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT a,b,c FROM update_test ORDER BY a,b,c;

--
-- Test VALUES in FROM
--

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

SELECT a,b,c FROM update_test ORDER BY a,b,c;

-- fail, wrong data type:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--

INSERT INTO update_test SELECT a,b+1,c FROM update_test;
SELECT * FROM update_test;

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT a,b,c FROM update_test ORDER BY a,b,c;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT a,b,c FROM update_test ORDER BY a,b,c;
-- fail, multi assignment to same column:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
SELECT * FROM update_test;
-- correlated sub-select:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
SELECT * FROM update_test;
-- fail, multiple rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
SELECT * FROM update_test;
-- *-expansion should work in this context:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
SELECT a, b, char_length(c) FROM update_test;

-- Test ON CONFLICT DO UPDATE

INSERT INTO upsert_test VALUES(1, 'Boo'), (3, 'Zoo');
-- uncorrelated  sub-select:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:
INSERT INTO upsert_test VALUES (1, 'Baz'), (3, 'Zaz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
-- correlated sub-select (EXCLUDED.* alias):
INSERT INTO upsert_test VALUES (1, 'Bat'), (3, 'Zot') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;

-- ON CONFLICT using system attributes in RETURNING, testing both the
-- inserting and updating paths. See bug report at:
-- https://www.postgresql.org/message-id/73436355-6432-49B1-92ED-1FE4F7E7E100%40finefun.com.au
INSERT INTO upsert_test VALUES (2, 'Beeble') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = 0 AS xmax_correct;
-- currently xmax is set after a conflict - that's probably not good,
-- but it seems worthwhile to have to be explicit if that changes.
INSERT INTO upsert_test VALUES (2, 'Brox') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = pg_current_xact_id()::xid AS xmax_correct;

DROP TABLE update_test;
DROP TABLE upsert_test;

-- Test ON CONFLICT DO UPDATE with partitioned table and non-identical children

CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
) PARTITION BY LIST (a);

CREATE TABLE upsert_test_1 PARTITION OF upsert_test FOR VALUES IN (1);
CREATE TABLE upsert_test_2 (b TEXT, a INT PRIMARY KEY);
ALTER TABLE upsert_test ATTACH PARTITION upsert_test_2 FOR VALUES IN (2);

INSERT INTO upsert_test VALUES(1, 'Boo'), (2, 'Zoo');
-- uncorrelated sub-select:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:
WITH aaa AS (SELECT 1 AS ctea, ' Foo' AS cteb) INSERT INTO upsert_test
  VALUES (1, 'Bar'), (2, 'Baz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT upsert_test.b||cteb, upsert_test.a FROM aaa) RETURNING *;

DROP TABLE upsert_test;


---------------------------
-- UPDATE with row movement
---------------------------

-- When a partitioned table receives an UPDATE to the partitioned key and the
-- new values no longer meet the partition's bound, the row must be moved to
-- the correct partition for the new partition key (if one exists). We must
-- also ensure that updatable views on partitioned tables properly enforce any
-- WITH CHECK OPTION that is defined. The situation with triggers in this case
-- also requires thorough testing as partition key updates causing row
-- movement convert UPDATEs into DELETE+INSERT.

CREATE TABLE range_parted (
	a text,
	b bigint,
	c numeric,
	d int,
	e varchar
) PARTITION BY RANGE (a, b);

-- Create partitions intentionally in descending bound order, so as to test
-- that update-row-movement works with the leaf partitions not in bound order.
CREATE TABLE part_b_20_b_30 (e varchar, c numeric, a text, b bigint, d int);
-- GPDB: distribution policy must match the parent table.
alter table part_b_20_b_30 set distributed by (a);
ALTER TABLE range_parted ATTACH PARTITION part_b_20_b_30 FOR VALUES FROM ('b', 20) TO ('b', 30);
CREATE TABLE part_b_10_b_20 (e varchar, c numeric, a text, b bigint, d int) PARTITION BY RANGE (c);
alter table part_b_10_b_20 set distributed by (a);
CREATE TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10);
ALTER TABLE range_parted ATTACH PARTITION part_b_10_b_20 FOR VALUES FROM ('b', 10) TO ('b', 20);
CREATE TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20);
CREATE TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10);

-- Check that partition-key UPDATE works sanely on a partitioned table that
-- does not have any child partitions.
UPDATE part_b_10_b_20 set b = b - 6;

-- Create some more partitions following the above pattern of descending bound
-- order, but let's make the situation a bit more complex by having the
-- attribute numbers of the columns vary from their parent partition.
CREATE TABLE part_c_100_200 (e varchar, c numeric, a text, b bigint, d int) PARTITION BY range (abs(d));
ALTER TABLE part_c_100_200 DROP COLUMN e, DROP COLUMN c, DROP COLUMN a;
ALTER TABLE part_c_100_200 ADD COLUMN c numeric, ADD COLUMN e varchar, ADD COLUMN a text;
ALTER TABLE part_c_100_200 DROP COLUMN b;
ALTER TABLE part_c_100_200 ADD COLUMN b bigint;
CREATE TABLE part_d_1_15 PARTITION OF part_c_100_200 FOR VALUES FROM (1) TO (15);
CREATE TABLE part_d_15_20 PARTITION OF part_c_100_200 FOR VALUES FROM (15) TO (20);

ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_100_200 FOR VALUES FROM (100) TO (200);

-- GPDB: distribution policy must match the parent table, so the previous command fails.
-- Change the distribution key and try again.
alter table part_c_100_200 set distributed by (a);
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_100_200 FOR VALUES FROM (100) TO (200);

CREATE TABLE part_c_1_100 (e varchar, d int, c numeric, b bigint, a text);
alter table part_c_1_100 set distributed by (a);
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_1_100 FOR VALUES FROM (1) TO (100);

\set init_range_parted 'truncate range_parted; insert into range_parted VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select tableoid::regclass::text COLLATE "C" partname, * from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
:init_range_parted;
:show_data;

-- The order of subplans should be in bound order
EXPLAIN (costs off) UPDATE range_parted set c = c - 50 WHERE c > 97;

-- fail, row movement happens only within the partition subtree.
UPDATE part_c_100_200 set c = c - 20, d = c WHERE c = 105;
-- fail, no partition key update, so no attempt to move tuple,
-- but "a = 'a'" violates partition constraint enforced by root partition)
UPDATE part_b_10_b_20 set a = 'a';
-- ok, partition key update, no constraint violation
UPDATE range_parted set d = d - 10 WHERE d > 10;
-- ok, no partition key update, no constraint violation
UPDATE range_parted set e = d;
-- No row found
UPDATE part_c_1_100 set c = c + 20 WHERE c = 98;
-- ok, row movement
UPDATE part_b_10_b_20 set c = c + 20 returning c, b, a;
:show_data;

-- fail, row movement happens only within the partition subtree.
UPDATE part_b_10_b_20 set b = b - 6 WHERE c > 116 returning *;
-- ok, row movement, with subset of rows moved into different partition.
UPDATE range_parted set b = b - 6 WHERE c > 116 returning a, b + c;

:show_data;

-- Common table needed for multiple test scenarios.
CREATE TABLE mintab(c1 int);
INSERT into mintab VALUES (120);

-- update partition key using updatable view.
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 FROM mintab) WITH CHECK OPTION;
-- ok
UPDATE upview set c = 199 WHERE b = 4;
-- fail, check option violation
UPDATE upview set c = 120 WHERE b = 4;
-- fail, row movement with check option violation
UPDATE upview set a = 'b', b = 15, c = 120 WHERE b = 4;
-- ok, row movement, check option passes
UPDATE upview set a = 'b', b = 15 WHERE b = 4;

:show_data;

-- cleanup
DROP VIEW upview;

-- RETURNING having whole-row vars.
:init_range_parted;
UPDATE range_parted set c = 95 WHERE a = 'b' and b > 10 and c > 100 returning (range_parted), *;
:show_data;


-- Creating default partition for range
:init_range_parted;
create table part_def partition of range_parted default;
\d+ part_def
insert into range_parted values ('c', 9);
-- ok
update part_def set a = 'd' where a = 'c';
-- fail
update part_def set a = 'a' where a = 'd';

:show_data;

-- Update row movement from non-default to default partition.
-- fail, default partition is not under part_a_10_a_20;
UPDATE part_a_10_a_20 set a = 'ad' WHERE a = 'a';
-- ok
UPDATE range_parted set a = 'ad' WHERE a = 'a';
UPDATE range_parted set a = 'bd' WHERE a = 'b';
:show_data;
-- Update row movement from default to non-default partitions.
-- ok
UPDATE range_parted set a = 'a' WHERE a = 'ad';
UPDATE range_parted set a = 'b' WHERE a = 'bd';
:show_data;

-- Cleanup: range_parted no longer needed.
DROP TABLE range_parted;

CREATE TABLE list_parted (
	a text,
	b int
) PARTITION BY list (a);
CREATE TABLE list_part1  PARTITION OF list_parted for VALUES in ('a', 'b');
CREATE TABLE list_default PARTITION OF list_parted default;
INSERT into list_part1 VALUES ('a', 1);
INSERT into list_default VALUES ('d', 10);

-- fail
UPDATE list_default set a = 'a' WHERE a = 'd';
-- ok
UPDATE list_default set a = 'x' WHERE a = 'd';

DROP TABLE list_parted;

--------------
-- Some more update-partition-key test scenarios below. This time use list
-- partitions.
--------------

-- Setup for list partitions
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
alter table sub_part1 set distributed by (a); -- GPDB: distribution policy must match the parent table.
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
alter table sub_part2 set distributed by (a); -- GPDB: distribution policy must match the parent table.
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.
UPDATE sub_parted set a = 2 WHERE c = 10;

-- Test update-partition-key, where the unpruned partitions do not have their
-- partition keys updated.
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;
UPDATE list_parted set b = c + a WHERE a = 2;
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;


-- Cleanup: list_parted no longer needed.
DROP TABLE list_parted;

-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);

create table hash_parted (
	a int,
	b int
) partition by hash (a custom_opclass, b custom_opclass);
create table hpart1 partition of hash_parted for values with (modulus 2, remainder 1);
create table hpart2 partition of hash_parted for values with (modulus 4, remainder 2);
create table hpart3 partition of hash_parted for values with (modulus 8, remainder 0);
create table hpart4 partition of hash_parted for values with (modulus 8, remainder 4);
insert into hpart1 values (1, 1);
insert into hpart2 values (2, 5);
insert into hpart4 values (3, 4);

-- fail
update hpart1 set a = 3, b=4 where a = 1;
-- ok, row movement
update hash_parted set b = b - 1 where b = 1;
-- ok
update hash_parted set b = b + 8 where b = 1;

-- cleanup
drop table hash_parted;
drop operator class custom_opclass using hash;
drop function dummy_hashint4(a int4, seed int8);
