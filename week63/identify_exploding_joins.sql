----------------------------------------
---------- query to inspect ------------
----------------------------------------
alter session set use_cached_result = false;

SELECT *
FROM t1
    LEFT JOIN t2 ON t1.value = t2.value
    LEFT JOIN t3 ON t1.value = t3.value
    LEFT JOIN t4 ON t1.value = t4.value;


-----------------------------------------------
---------- identify exploding join ------------
-----------------------------------------------
set qid = last_query_id();

create or replace function identify_explosive_join(target_query_id string)
    returns table (
        guilty_join variant
        , row_multiplier float
    )
as 
$$
with 
query_operator_stats as
(
    select *
    from table(get_query_operator_stats(target_query_id))
)
, join_operators as
(
    select *
    from query_operator_stats
    where operator_type = 'Join'
)
, join_child as
(
    select all_op.*
    from query_operator_stats as all_op
        , join_operators as join_op
    where all_op.parent_operators[0] = join_op.operator_id
)
, exploding_join as
(
    select any_value(join_operators.operator_attributes:equality_join_condition) as guilty_join 
        , max(join_child.operator_statistics:output_rows) as max_join_input_rows
        , any_value(join_operators.operator_statistics:output_rows) as join_output_rows
        , join_output_rows/max_join_input_rows as row_multiplier
    from join_child
    inner join join_operators
        on join_child.parent_operators[0] = join_operators.operator_id 
    group by join_child.parent_operators
)

select guilty_join
    , row_multiplier
from exploding_join
where row_multiplier > 1
order by row_multiplier
$$
;


select *
from table(identify_explosive_join($qid));
