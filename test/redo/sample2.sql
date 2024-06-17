-- use 0
drop table t_redo;
create table t_redo(id int, age int);
begin;
insert into t_redo values (0, 0);
insert into t_redo values (1, 0);
insert into t_redo values (2, 0);
insert into t_redo values (3, 0);
commit;
bye
-- use 1
begin;
insert into t_redo values (4, 1);
insert into t_redo values (5, 1);
insert into t_redo values (6, 1);
delete from t_redo where id = 0;
update t_redo set age = 1 where id = 1;
-- use 2
begin;
insert into t_redo values (7, 2);
delete from t_redo where id = 2;
update t_redo set age = 2 where id = 3;
-- use 1
commit;
bye
-- use 3
begin;
insert into t_redo values (7, 3);
insert into t_redo values (8, 3);
insert into t_redo values (9, 3);
delete from t_redo where id = 4;
update t_redo set age = 3 where id = 5;
update t_redo set age = 999 where id = 9;
delete from t_redo where id = 9;
-- use 2
rollback;
bye
-- use 4
begin;
update t_redo set age = 4 where id = 2;
-- use 3
commit;
bye
-- use 5
begin;
insert into t_redo values (10, 5);
delete from t_redo where id = 7;
update t_redo set age = 5 where id = 8;
-- use 4
commit;
bye
-- use 6
begin;
update t_redo set age = 6 where id < 7;
delete from t_redo where id < 7;
insert into t_redo values (11, 6);
-- use 7
select * from t_redo;
bye
-- use 5
bye
-- use 6
bye
