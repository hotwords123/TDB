CREATE TABLE join_table_1(id int, name char);
CREATE TABLE join_table_2(id int, num int);
CREATE TABLE join_table_3(id int, num2 int);
create table join_table_empty_1(id int, num_empty_1 int);
create table join_table_empty_2(id int, num_empty_2 int);

INSERT INTO join_table_1 VALUES (1, 'a');
INSERT INTO join_table_1 VALUES (2, 'b');
INSERT INTO join_table_1 VALUES (3, 'c');
INSERT INTO join_table_2 VALUES (1, 2);
INSERT INTO join_table_2 VALUES (2, 15);
INSERT INTO join_table_3 VALUES (1, 120);
INSERT INTO join_table_3 VALUES (3, 800);

Select * from join_table_1 inner join join_table_2 on join_table_1.id=join_table_2.id;
Select join_table_1.name from join_table_1 inner join join_table_2 on join_table_1.id=join_table_2.id;
Select join_table_2.num from join_table_1 inner join join_table_2 on join_table_1.id=join_table_2.id;
Select * from join_table_1 inner join join_table_2 on join_table_1.id=join_table_2.id inner join join_table_3 on join_table_1.id=join_table_3.id;
