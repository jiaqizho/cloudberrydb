set default_table_access_method = pax;

set pax_enable_toast to true;

-- test compress failed
-- random varchar always make compress toast failed
create or replace function random_string(integer)
returns text as
$body$
    select upper(array_to_string(array(select substring('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' 
    FROM (ceil(random()*62))::int FOR 1) FROM generate_series(1, $1)), ''));
$body$
language sql volatile;

create table pax_toasttest_compress_failed(v text);
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));

select length(v) from pax_toasttest_compress_failed;
drop function random_string;
drop table pax_toasttest_compress_failed;
