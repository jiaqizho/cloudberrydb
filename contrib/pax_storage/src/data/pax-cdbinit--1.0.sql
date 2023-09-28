-- insert pax catalog values
INSERT INTO pg_proc VALUES(7600,'pax_tableam_handler',11,10,13,1,0,0,0,'f','f','f','t','f','s','u',1,0,269,'2281',null,null,null,null,null,'pax_tableam_handler','$libdir/pax',null,null,null,'n','a');
INSERT INTO pg_am   VALUES(7014,'pax',7600,'t');
COMMENT ON FUNCTION pax_tableam_handler IS 'column-optimized PAX table access method handler';
INSERT INTO pg_proc VALUES(7601,'paxauxstats_in',11,10,13,1,0,0,0,'f','f','f','t','f','i','u',1,0,7603,'2275',null,null,null,null,null,'MicroPartitionStatsInput','$libdir/pax',null,null,null,'n','a');
INSERT INTO pg_proc VALUES(7602,'paxauxstats_out',11,10,13,1,0,0,0,'f','f','f','t','f','i','u',1,0,2275,'7603',null,null,null,null,null,'MicroPartitionStatsOutput','$libdir/pax',null,null,null,'n','a');
INSERT INTO pg_type VALUES(7603,'paxauxstats',11,10,-1,'f','b','U','f','t',',',0,0,0,0,7601,7602,0,0,0,0,0,'i','x','t',0,-1,0,0,null,null,null);
-- create pax auxiliary fast sequence table
create table pg_paxaux.pg_pax_fastsequence(objid oid not null, seq bigint not null);
create index pg_pax_fastsequence_objid_idx on pg_paxaux.pg_pax_fastsequence(objid);
