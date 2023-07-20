-- insert pax catalog values
INSERT INTO pg_proc VALUES(7600,'pax_tableam_handler',11,10,13,1,0,0,0,'f','f','f','t','f','s','u',1,0,269,'2281',null,null,null,null,null,'pax_tableam_handler','$libdir/pax',null,null,null,'n','a');
INSERT INTO pg_am   VALUES(7014,'pax',7600,'t');
COMMENT ON FUNCTION pax_tableam_handler IS 'column-optimized PAX table access method handler';
