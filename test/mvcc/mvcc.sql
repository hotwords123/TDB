-- use 0
drop table t_mvcc;
create table t_mvcc(id int, age int, name char(4));
bye
-- use 1
begin;
-- use 2
begin;
-- use 1
insert into t_mvcc values(1, 1, 'a');
insert into t_mvcc values(2, 2, 'b');
select * from t_mvcc;
-- use 2
select * from t_mvcc;
-- use 1
commit;
bye
-- use 2
select * from t_mvcc;
-- use 3
begin;
select * from t_mvcc;
-- use 2
delete from t_mvcc where id=1;
commit;
bye
-- use 4
begin;
-- use 3
delete from t_mvcc where id=1;
-- use 4
select * from t_mvcc;
-- use 3
commit;
bye
-- use 4
select * from t_mvcc;
commit;
bye
-- use 5
begin;
select * from t_mvcc;
commit;
bye
