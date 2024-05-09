----------------------------------------------------------------------
-- DATABASE, SCHEMA AND TABLE CREATION
----------------------------------------------------------------------
create database if not exists db_ff;
create or replace schema db_ff.week5;

create table db_ff.week5.mock_data as
SELECT uniform(1, 10, RANDOM(12)) as col1
  FROM TABLE(GENERATOR(ROWCOUNT => 1)) 
  ORDER BY 1;

----------------------------------------------------------------------
-- UDF CREATION
----------------------------------------------------------------------
create or replace function xthree(i int)
returns int
language python
runtime_version = '3.11'
handler = 'xthree_py'
as
$$
def xthree_py(i):
  return i*3
$$;


----------------------------------------------------------------------
-- UDF EXAMPLE
----------------------------------------------------------------------
select xthree(i => col1)
from db_ff.week5.mock_data;
