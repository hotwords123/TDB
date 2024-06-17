select * from join_table_1 inner join join_table_empty_1 on join_table_1.id=join_table_empty_1.id;
select * from join_table_empty_1 inner join join_table_1 on join_table_empty_1.id=join_table_1.id;
select * from join_table_empty_1 inner join join_table_empty_2 on join_table_empty_1.id = join_table_empty_2.id;
select * from join_table_1 inner join join_table_2 on join_table_1.id = join_table_2.id inner join join_table_empty_1 on join_table_1.id=join_table_empty_1.id;
select * from join_table_empty_1 inner join join_table_1 on join_table_empty_1.id=join_table_1.id inner join join_table_2 on join_table_1.id=join_table_2.id;
