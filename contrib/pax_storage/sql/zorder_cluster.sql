-- test cluster_columns
set pax_max_tuples_per_file to 131072;
drop table if EXISTS t_zorder_cluster;
create table t_zorder_cluster(c1 int, c2 int) using pax with(minmax_columns='c1,c2');
\d+ t_zorder_cluster;

insert into t_zorder_cluster select i,i from generate_series(1,100000) i;
insert into t_zorder_cluster select i,i from generate_series(1,100000) i;
insert into t_zorder_cluster select i,i from generate_series(1,100000) i;
insert into t_zorder_cluster select i,i from generate_series(1,100000) i;
insert into t_zorder_cluster select i,i from generate_series(1,100000) i;

select ptblockname,ptstatistics,ptisclustered from get_pax_aux_table('t_zorder_cluster');

-- error, becuase no cluster index or cluster_columns is defined
cluster t_zorder_cluster;

alter table t_zorder_cluster set(cluster_columns='c1,c2');
\d+ t_zorder_cluster;
-- success
cluster t_zorder_cluster;
select ptblockname,ptstatistics,ptisclustered from get_pax_aux_table('t_zorder_cluster');


drop table t_zorder_cluster;

-- test cluster index
set pax_max_tuples_per_file to 131072;
drop table if EXISTS t_index_cluster;
create table t_index_cluster(c1 int, c2 int) using pax with(minmax_columns='c1,c2');
\d+ t_index_cluster;

insert into t_index_cluster select i,i from generate_series(1,100000) i;
insert into t_index_cluster select i,i from generate_series(1,100000) i;
insert into t_index_cluster select i,i from generate_series(1,100000) i;
insert into t_index_cluster select i,i from generate_series(1,100000) i;
insert into t_index_cluster select i,i from generate_series(1,100000) i;

select ptblockname,ptstatistics,ptisclustered from get_pax_aux_table('t_index_cluster');

-- error, becuase no cluster index or cluster_columns is defined
cluster t_index_cluster;

create index idx_t_index_cluster on t_index_cluster(c1);
\d+ t_index_cluster;

-- success
cluster t_index_cluster using idx_t_index_cluster;

select ptblockname,ptstatistics,ptisclustered from get_pax_aux_table('t_index_cluster');
drop table t_index_cluster;

-- test both cluster index and cluster columns
create table t_both_zorder_and_index_cluster(c1 int, c2 int) using pax with(minmax_columns='c1,c2');
alter table t_both_zorder_and_index_cluster set(cluster_columns='c1,c2');
create index idx_t_both_zorder_and_index_cluster on t_both_zorder_and_index_cluster(c1);
-- error, because zorder cluster and index cluster can not be used together
cluster t_both_zorder_and_index_cluster using idx_t_both_zorder_and_index_cluster;
-- success
cluster t_both_zorder_and_index_cluster;
alter table t_both_zorder_and_index_cluster set(cluster_columns='');
-- error, because zorder cluster reloptions has been deleted
cluster t_both_zorder_and_index_cluster;
-- success
cluster t_both_zorder_and_index_cluster using idx_t_both_zorder_and_index_cluster;
-- error
alter table t_both_zorder_and_index_cluster set(cluster_columns='c1,c2');
drop index idx_t_both_zorder_and_index_cluster;
-- error
cluster t_both_zorder_and_index_cluster;
-- success
alter table t_both_zorder_and_index_cluster set(cluster_columns='c1,c2');
-- success
cluster t_both_zorder_and_index_cluster;

-- test unsupport type

create table t_zorder_unsupport_type(c1 int, c2 numeric(10,2),c3 varchar(128), c4 timestamp, c5 bpchar(64) ) using pax with(minmax_columns='c1,c2');

-- error, because numeric is unsupport type
alter table t_zorder_unsupport_type set(cluster_columns='c1,c2');
alter table t_zorder_unsupport_type set(cluster_columns='c1,c3');
-- error, because timestamp is unsupport type
alter table t_zorder_unsupport_type set(cluster_columns='c1,c4');
alter table t_zorder_unsupport_type set(cluster_columns='c1,c5');
