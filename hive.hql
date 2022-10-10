with
    initiatial_table_1 as (
        select *
        from
            clickstream
        where
            event_type != 'event'
    ),
    table_2 as (
        select
            concat(user_id, '_', session_id) as id,
            event_type,
            event_page,
            timestamp
        from
            initiatial_table_1
    ),
    table_3 as (
        select
            id,
            min(timestamp) first_error_timestamp
        from
            table_2
        where
            event_type like '%error%'
        group by
            id
    ),
    table_4 as (
        select
            table_2.id id,
            table_2.event_page event_page,
            table_2.timestamp timestamp
        from
            table_2
            left join table_3
                on table_2.id = table_3.id
        where
            table_2.timestamp < table_3.first_error_timestamp or table_3.first_error_timestamp is null
    ),
    table_5 as (
        select
            sort_array(collect_list(named_struct('timestamp', timestamp, 'event_page', event_page))) as pages
        from
            table_4
        group by
            id
    ),
    table_6 as (
        select
            concat_ws('-', pages.event_page) as route
        from
            table_5
    ),
    final_table as (
        select
            route,
            count(*) as count
        from
            table_6
        group by
            route
        order by
            count desc
    )
select * from final_table limit 30;
