-- use 0
drop table t_mvcc;
create table t_mvcc(id int, age int, name char(4));
insert into t_mvcc values (1, 1, 'a');
insert into t_mvcc values (2, 2, 'b');
select * from t_mvcc;
bye
-- use 1
begin;
insert into t_mvcc values (3, 3, 'c');
delete from t_mvcc where id = 1;
select * from t_mvcc;
-- use 2
begin;
select * from t_mvcc;
-- use 1
commit;
bye
-- use 2
insert into t_mvcc values (4, 4, 'd');
delete from t_mvcc where id = 2;
select * from t_mvcc;
-- use 3
begin;
select * from t_mvcc;
-- use 2
commit;
bye
-- use 3
insert into t_mvcc values (5, 5, 'e');
delete from t_mvcc where id = 3;
delete from t_mvcc where id = 4;
select * from t_mvcc;
-- use 4
begin;
select * from t_mvcc;
insert into t_mvcc values (6, 6, 'f');
delete from t_mvcc where id = 5;
select * from t_mvcc;
-- use 3
commit;
bye
-- use 4
select * from t_mvcc;
commit;
bye
-- use 5
select * from t_mvcc;
bye
