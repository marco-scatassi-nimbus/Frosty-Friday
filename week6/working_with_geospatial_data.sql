----------------------------------------------------------------------
-- DATABASE, SCHEMA AND TABLES (through snowsight) CREATION
----------------------------------------------------------------------
create database if not exists db_ff;
create or replace schema db_ff.week6;


----------------------------------------------------------------------
-- CONVERT COLUMNS longitude, latitude IN A SINGLE GEOGRAPHY COLUMN
----------------------------------------------------------------------
-----------------------------
-- westminster_constituency
-----------------------------
alter table db_ff.week6.westminster_constituency
add column if not exists coordinates geography;

update db_ff.week6.westminster_constituency
set coordinates = st_makepoint(longitude, latitude);

-----------------------------
-- nation_and_region
-----------------------------
alter table db_ff.week6.nation_and_region
add column if not exists coordinates geography;

update db_ff.week6.nation_and_region
set coordinates = st_makepoint(longitude, latitude);


----------------------------------------------------------------------
-- OBTAINING POLYGONS 
----------------------------------------------------------------------
-----------------------------
-- westminster_constituency
-----------------------------
create or replace table db_ff.week6.westminster_constituency_polygon as
    with 
    starting_point as
        (
            select * exclude(coordinates), coordinates as starting_point
            from db_ff.week6.westminster_constituency
            where sequence_num = 0
        )
    , other_points as
        (
            select constituency, part, st_collect(coordinates) as multipoint_minus_start
            from db_ff.week6.westminster_constituency
            where sequence_num != 0
            group by constituency, part
        )
    , final as 
        (
            select constituency, part
                , st_polygon(st_makeline(starting_point, multipoint_minus_start)) as polygon
            from starting_point
            inner join other_points using (constituency, part)
        )
    
    select constituency, st_collect(polygon) as polygons
    from final
    group by constituency;


-----------------------------
-- nation_and_region
-----------------------------
create or replace table db_ff.week6.nation_and_region_polygon as
    with 
    starting_point as
        (
            select * exclude(coordinates), coordinates as starting_point
            from db_ff.week6.nation_and_region
            where sequence_num = 0
        )
    , other_points as
        (
            select nation_or_region_name, type, part, st_collect(coordinates) as multipoint_minus_start
            from db_ff.week6.nation_and_region
            where sequence_num != 0
            group by nation_or_region_name, type, part
        )
    , final as 
        (
            select nation_or_region_name, type, part
                , st_polygon(st_makeline(starting_point, multipoint_minus_start)) as polygon
            from starting_point
            inner join other_points using (nation_or_region_name, type, part)
        )
    
    select nation_or_region_name, type, st_collect(polygon) as polygons 
    from final
    group by nation_or_region_name, type;


----------------------------------------------------------------------
-- GET INTERSECTIONS 
----------------------------------------------------------------------
select nation_or_region_name
    , count(constituency) as constituency_whitin_nation_or_region
from db_ff.week6.nation_and_region_polygon as nar
    cross join db_ff.week6.westminster_constituency_polygon as wc
where st_intersects(nar.polygons, wc.polygons)
group by nation_or_region_name
order by constituency_whitin_nation_or_region desc;
