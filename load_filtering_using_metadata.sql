-- FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. 
-- This data is needed for analysis. 
-- Your task is to create an external stage, and load the csv files directly from that stage into a table.

-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/

----------------------------------------------------------------------
-- DATABASE, SCHEMA AND TABLE CREATION
----------------------------------------------------------------------
create database if not exists db_ff;
create or replace schema db_ff.week3;
create or replace table db_ff.week3.relevant_files
    (filename varchar
    , number_of_rows int);

----------------------------------------------------------------------
-- EXTERNAL STAGE CREATION
----------------------------------------------------------------------
create or replace stage db_ff.week3.s3_stage_ff
    url = 's3://frostyfridaychallenges/challenge_3/'
    directory = (enable = TRUE);


----------------------------------------------------------------------
-- CREATE A PROPER FILE FORMAT
----------------------------------------------------------------------
create or replace file format db_ff.week3.myCSVfileformat
    type = csv
    skip_header = 1;


----------------------------------------------------------------------
-- USING SCRIPTING WITHIN AN ANONIMOUS BLOCK TO INSERT FILES 
-- ONLY IF RELEVANT IN THE PREVIOUSLY CREATED TABLE
----------------------------------------------------------------------
declare
  file_name varchar;
  row_count int;
  c CURSOR for 
    select t.$1 
    from @db_ff.week3.s3_stage_ff (file_format => 'db_ff.week3.myCSVfileformat', pattern => 'keywords[.]csv') t;
begin
  for record in c DO
    let patt1 := concat('%', record.$1, '%');
    let patt2 := concat('.*', record.$1, '.*[.]csv');
    let res1 resultset := (
                    select relative_path 
                    from directory(@db_ff.week3.s3_stage_ff)
                    where relative_path ilike :patt1
                 );
    let c1 cursor for res1; 
    open c1;
    fetch c1 into file_name;
    let res2 resultset := (
                    select count(t.$1) 
                    from @db_ff.week3.s3_stage_ff (file_format => 'db_ff.week3.myCSVfileformat', pattern => :patt2) t
                 );
    let c2 cursor for res2;
    open c2;
    fetch c2 into row_count;
    
    insert into db_ff.week3.relevant_files (filename, number_of_rows)
        values (:file_name, :row_count);
  end for;
end;

----------------------------------------------------------------------
-- VIEW THE RESULT
----------------------------------------------------------------------
select * 
from db_ff.week3.relevant_files;



