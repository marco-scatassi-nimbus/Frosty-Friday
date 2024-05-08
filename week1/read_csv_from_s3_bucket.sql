-- FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. 
-- This data is needed for analysis. 
-- Your task is to create an external stage, and load the csv files directly from that stage into a table.

-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/

----------------------------------------------------------------------
-- DATABASE AND SCHEMA CREATION
----------------------------------------------------------------------
create or replace database db_ff;
create or replace schema db_ff.week1;

----------------------------------------------------------------------
-- EXTERNAL STAGE CREATION
----------------------------------------------------------------------
create or replace stage db_ff.week1.s3_stage_ff
    url = 's3://frostyfridaychallenges/challenge_1/';

    
----------------------------------------------------------------------
-- FILES INSPECTION
----------------------------------------------------------------------
list @s3_stage_ff;

select t.$1 from @s3_stage_ff (pattern => '.*[.]csv') t;


----------------------------------------------------------------------
-- TABLE CREATION AND DATA LAODING
----------------------------------------------------------------------
create or replace table db_ff.week1.s3_data
    (col1 varchar);

copy into db_ff.week1.s3_data
from @db_ff.week1.s3_stage_ff
pattern = '.*[.]csv'
file_format = (type = 'csv');


----------------------------------------------------------------------
-- QUERYING DATA
----------------------------------------------------------------------
select * from db_ff.week1.s3_data;


