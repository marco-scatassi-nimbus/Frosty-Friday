-- Frosty Friday Consultants has been hired by the University of Frost’s history department; 
-- they want data on monarchs in their data warehouse for analysis. 
-- Your job is to take the JSON file located here, ingest it into the data warehouse, and parse it into a table

-- Separate columns for nicknames and consorts 1 – 3, many will be null.
-- An ID in chronological order (birth).
-- An Inter-House ID in order as they appear in the file.
-- There should be 26 rows at the end.
-- Hints:

-- Make sure you don’t lose any rows along the way.
-- Be sure to investigate all the outputs and parameters available when transforming JSON.


----------------------------------------------------------------------
-- DATABASE, SCHEMA AND TABLE CREATION
----------------------------------------------------------------------
create database if not exists db_ff;
create or replace schema db_ff.week4;


----------------------------------------------------------------------
-- INTERNAL STAGE CREATION
----------------------------------------------------------------------
create or replace stage db_ff.week4.my_stage_ff
    directory = (enable = TRUE);


----------------------------------------------------------------------
-- LOAD THE JSON USING SNOWSQL
----------------------------------------------------------------------


----------------------------------------------------------------------
-- CREATE THE RAW TABLE USING SCHEMA INFERENCE
----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT db_ff.week4.my_json_format
  TYPE = json
  STRIP_OUTER_ARRAY = true;

CREATE OR REPLACE FILE FORMAT db_ff.week4.my_json_format2
  TYPE = json;

  
CREATE OR REPLACE TABLE db_ff.week4.monarch_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(        
            LOCATION=>'@db_ff.week4.my_stage_ff/'
            , FILE_FORMAT => 'db_ff.week4.my_json_format'
            , FILES => 'monarch.json.gz'
        )
    ));


copy into db_ff.week4.monarch_raw
from @db_ff.week4.my_stage_ff
file_format = 'db_ff.week4.my_json_format'
match_by_column_name = case_insensitive;

----------------------------------------------------------------------
-- CREATE A NEW FORMATTED TABLE 
----------------------------------------------------------------------
create or replace table db_ff.week4.monarch as
(
    SELECT 
        "Era" AS Era,
        t2.value:"House"::varchar AS House,
        t3.value:"Name"::varchar AS Name,
        t3.value:"Nickname"::array[0] AS Nickname_1,
        t3.value:"Nickname"::array[1] AS Nickname_2,
        t3.value:"Nickname"::array[2] AS Nickname_3,
        t3.value:"Birth"::date AS Birth,
        t3.value:"Start of Reign"::date AS Start_of_Reign,
        t3.value:"End of Reign"::date AS End_of_Reign,
        t3.value:"Duration"::varchar AS Duration,
        t3.value:"Death"::date AS Death,
        t3.value:"Consort\/Queen Consort"::array[0] AS Consort_Queen_Consort_1,
        t3.value:"Consort\/Queen Consort"::array[1] AS Consort_Queen_Consort_2,
        t3.value:"Consort\/Queen Consort"::array[2] AS Consort_Queen_Consort_3,
        t3.value:"Place of Birth"::varchar AS Place_of_Birth,
        t3.value:"Place of Death"::varchar AS Place_of_Death,
        t3.value:"Age at Time of Death"::varchar AS Age_at_Time_of_Death,
        t3.value:"Burial Place"::varchar AS Burial_Place
    FROM 
        db_ff.week4.monarch_raw t1
        , LATERAL FLATTEN(t1."Houses") t2
        , LATERAL FLATTEN(t2.value:"Monarchs") t3
);


----------------------------------------------------------------------
-- VIEW THE RESULT
----------------------------------------------------------------------
select *
from db_ff.week4.monarch;
