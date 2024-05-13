----------------------------------------------------------------------
-- INSPECT ACCOUNT USAGE 
----------------------------------------------------------------------
with 
acc_query_hist as
(
    select query_hist.*
        , acc_hist_flat.value as obj_details
    from snowflake.account_usage.query_history query_hist
    left join snowflake.account_usage.access_history acc_hist
        on query_hist.query_id = acc_hist.query_id
        , lateral flatten(acc_hist.direct_objects_accessed) acc_hist_flat
)
, acc_query_tag_hist as
(
    select *
    from acc_query_hist
    inner join snowflake.account_usage.tag_references tag_ref 
        on split(obj_details:"objectName", '.')[2] = tag_ref.object_name
    where tag_value = 'Level Super Secret A+++++++'
)
, final as
(
    select tag_name
        , tag_value
        , any_value(query_id)
        , object_name as table_name
        , role_name
    from acc_query_tag_hist
    group by tag_name, tag_value, table_name, role_name
)

select * from final;
