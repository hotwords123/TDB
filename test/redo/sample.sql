-- use 0
drop table t_redo;
create table t_redo(id int, age int);
bye
-- use 1
begin;
insert into t_redo values(0, 0);
insert into t_redo values(1, 10);
commit;
bye
-- use 2
begin;
insert into t_redo values(2, 20);
-- use 3
begin;
-- use 2
commit;
bye
-- use 3
insert into t_redo values(3, 30);
-- use 4
begin;
insert into t_redo values(4, 40);
delete from t_redo where id = 0;
-- use 3
insert into t_redo values(5, 50);
bye
-- use 4
bye
