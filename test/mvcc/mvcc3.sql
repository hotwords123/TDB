-- use 0
drop table t_mvcc;
create table t_mvcc(id int, age int, name char(4));
insert into t_mvcc values (1, 1, 'a');
insert into t_mvcc values (2, 2, 'b');
insert into t_mvcc values (3, 3, 'c');
select * from t_mvcc;
bye
-- use 1
begin;
-- use 2
begin;
-- use 1
update t_mvcc set age = 4 where id = 1;
-- use 2
update t_mvcc set age = 5 where id = 2;
-- use 1
update t_mvcc set age = 6 where id = 3;
-- use 2
update t_mvcc set age = 7 where id = 2;
-- use 1
update t_mvcc set age = 8 where id = 2;
-- use 2
commit;
bye
-- use 1
rollback;
bye
-- use 3
select * from t_mvcc;
bye
