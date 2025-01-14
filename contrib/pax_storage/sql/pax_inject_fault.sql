-- start_ignore
drop table if exists t1;
-- end_ignore
create table t1(a int);
select gp_inject_fault('orc_writer_write_tuple','panic',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
-- failed because of fault injection
insert into t1 select generate_series(1,10);
-- success 
insert into t1 select generate_series(1,10);
select gp_inject_fault('orc_writer_write_tuple','reset',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
drop table t1;