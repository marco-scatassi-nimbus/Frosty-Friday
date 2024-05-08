-- A stakeholder in the HR department wants to do some change-tracking but is concerned that 
-- the stream which was created for them gives them too much info they don’t care about.

-- Load in the parquet data and transform it into a table, then create a stream that will only 
-- show us changes to the DEPT and JOB_TITLE columns. 

----------------------------------------------------------------------
-- DATABASE AND SCHEMA CREATION
----------------------------------------------------------------------
create database if not exists db_ff;
create schema if not exists db_ff.week2;


----------------------------------------------------------------------
-- DATA LOADING (through snowsight)
----------------------------------------------------------------------


----------------------------------------------------------------------
-- DATA QUERYING
----------------------------------------------------------------------
select * from db_ff.week2.employee;


----------------------------------------------------------------------
-- CREATE A VIEW TO SELECT FIELDS TO TRACK
----------------------------------------------------------------------
create or replace view db_ff.week2.filtered_employee
as 
    select employee_id, dept, job_title
    from db_ff.week2.employee;

    
----------------------------------------------------------------------
-- CREATE A STREAM ON THE VIEW
----------------------------------------------------------------------
create or replace stream filtered_employee_stream on view db_ff.week2.filtered_employee;


----------------------------------------------------------------------
-- UPDATE TABLE
----------------------------------------------------------------------
UPDATE db_ff.week2.employee SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE db_ff.week2.employee SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE db_ff.week2.employee SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE db_ff.week2.employee SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE db_ff.week2.employee SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;


----------------------------------------------------------------------
-- QUERY THE STREAM
----------------------------------------------------------------------
select * from DB_FF.WEEK2.FILTERED_EMPLOYEE_STREAM;
