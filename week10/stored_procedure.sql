-- Create the warehouses
create warehouse if not exists my_xsmall_wh 
    with 
        warehouse_size = XSMALL
        auto_suspend = 60;
    
create warehouse if not exists my_small_wh 
    with 
        warehouse_size = SMALL
        auto_suspend = 60;

-- Create the schema
create or replace schema db_ff.week10;
        
-- Create the table
create or replace table db_ff.week10.transaction
(
    date_time datetime,
    trans_amount double
);

-- Create the stage
create or replace stage week_10_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_10/'
    file_format = (type='csv', skip_header=1)
    directory = (enable=true);

    
-- Create the stored procedure
create or replace procedure dynamic_warehouse_data_load(stage_name string, table_name string)
    returns text
    language sql
    execute as caller
    as
    declare 
        sql_query varchar;
        loaded_total int := 0;
    begin
        let query := 'select relative_path, size from directory(@' || stage_name || ');';  
        let rs resultset := (execute immediate :query);
        let c cursor for rs;
        
        for record in c DO
            let relative_name varchar := record.relative_path;
            let size int := record.size;
            
            if (size < 10000) then 
                execute immediate 'use warehouse my_xsmall_wh';
            else 
                execute immediate 'use warehouse my_small_wh';
            end if;

            sql_query := 'copy into ' || table_name || ' from @' || stage_name || ' pattern = \'.*' || :relative_name || '\'';
            execute immediate sql_query; 
        end for;
        
        select count(*) into :loaded_total from identifier(:table_name);
        return loaded_total || ' rows were added';
    end;

-- Call the stored procedure.
call dynamic_warehouse_data_load('db_ff.week10.week_10_frosty_stage', 'db_ff.week10.transaction');
