create table t_dict1(a int, b text) using pax with(storage_format=porc,compresstype=dict);

insert into t_dict1 select 1, repeat('1b', 12345678) from generate_series(1,20)i;

set vector.enable_vectorization=false;
explain select count(*) from t_dict1;
select count(*) from t_dict1;

explain select count(b) from t_dict1;
select count(b) from t_dict1;

set vector.enable_vectorization=true;
explain select count(*) from t_dict1;
select count(*) from t_dict1;

explain select count(b) from t_dict1;
select count(b) from t_dict1;

drop table t_dict1;

create table t_dict1(a int, b text) using pax with(storage_format=porc_vec,compresstype=dict);

insert into t_dict1 select 1, repeat('1b', 12345678) from generate_series(1,20)i;

set vector.enable_vectorization=false;
explain select count(*) from t_dict1;
select count(*) from t_dict1;

explain select count(b) from t_dict1;
select count(b) from t_dict1;

set vector.enable_vectorization=true;
explain select count(*) from t_dict1;
select count(*) from t_dict1;

explain select count(b) from t_dict1;
select count(b) from t_dict1;

drop table t_dict1;
