SELECT * FROM student
INNER JOIN teacher ON student.tutor_id = teacher.id
WHERE student.name = 'Alice';

SELECT * FROM a
INNER JOIN b ON a.id = b.id
INNER JOIN c ON a.id = c.id;

select * from join_table_large_1
inner join join_table_large_2 on join_table_large_1.id=join_table_large_2.id
inner join join_table_large_3 on join_table_large_1.id=join_table_large_3.id
inner join join_table_large_4 on join_table_large_3.id=join_table_large_4.id
inner join join_table_large_5 on 1=1
inner join join_table_large_6 on join_table_large_5.id=join_table_large_6.id
where join_table_large_3.num3 <10 and join_table_large_5.num5>90;
