with sessions as (

    select
        user_session,
        min(user_id) as user_id,
        min(brand) as brand,
        min(category_code) as category_code,
        count(*) as total_activity_count,
        count_if(event_type = 'view') as view_count,
        count_if(event_type = 'cart') as cart_add_count,
        count_if(event_type = 'purchase') as purchase_count,
        max(case when event_type = 'purchase' then 1 else 0 end) as converted,
        min(event_time) as session_start_time,
        max(event_time) as session_end_time,
        unix_timestamp(max(event_time))
            - unix_timestamp(min(event_time)) as session_duration_seconds,
        pmod(dayofweek(min(event_time)) - 1, 7) as day_of_week,
        hour(min(event_time)) as hour_of_day,
        min(case when event_type = 'view' then event_time end)
            as first_view_time,
        min(case when event_type = 'cart' then event_time end)
            as first_cart_time
    from {{ ref('stg_events') }}
    group by user_session

)

select
    *,
    case
        when first_view_time is not null and first_cart_time is not null
            then unix_timestamp(first_cart_time)
                - unix_timestamp(first_view_time)
    end as seconds_to_first_cart
from sessions
