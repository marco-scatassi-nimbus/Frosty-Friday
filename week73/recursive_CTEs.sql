use database db_ff;
create or replace schema week73;

CREATE OR REPLACE table departments (department_name varchar, department_ID int, head_department_ID int);

INSERT INTO departments (department_name, department_ID, head_department_ID) VALUES
    ('Research & Development', 1, NULL),  -- The Research & Development department is the top level.
        ('Product Development', 11, 1),
            ('Software Design', 111, 11),
            ('Product Testing', 112, 11),
        ('Human Resources', 2, 1),
            ('Recruitment', 21, 2),
            ('Employee Relations', 22, 2);

            
-- manual iteration
select '-> ' 
    || iff(d3.department_name is null, '', d3.department_name || ' -> ')
    || iff(d2.department_name is null, '', d2.department_name || ' -> ')
    || d1.department_name                                                  as connection_tree
    , d1.*
from departments d1
left join departments d2
    on d1.head_department_id = d2.department_id
left join departments d3 
    on d2.head_department_id = d3.department_id
order by length(connection_tree);


-- with recursive statement
with recursive lineage as (
    select ' -> ' ||  department_name as connection_tree
        , department_name
        , department_id
        , head_department_id as iterative_department_id
        , head_department_id as head_department_id
    from departments 
    union all
    select  ' -> ' ||  d.department_name || l.connection_tree as connection_tree
        , l.department_name
        , l.department_id
        , d.head_department_id as iterative_department_id
        , l.head_department_id as head_department_id
    from departments d
    join lineage l 
        on l.iterative_department_id = d.department_id
)
select * exclude(iterative_department_id)
from lineage
where iterative_department_id is null
order by department_id, head_department_id;
