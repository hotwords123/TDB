create table index_lab(id int, name char, score float);
create index i_id on index_lab(id);
load data infile "bin/data.txt" into table index_lab;
