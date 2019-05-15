create table test (name varchar, age int);
load data infile "mytable.csv" into table test;
select name, max(age) as x from test group by name;