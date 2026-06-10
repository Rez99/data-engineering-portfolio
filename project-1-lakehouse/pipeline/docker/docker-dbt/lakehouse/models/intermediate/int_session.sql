-- models/intermediate/int_session.sql

{{ config(
    materialized='table',
    database='polaris',
    schema='silver'
) }}

with cleaned as (

    select *
/*
        event_time,
        event_type,
        product_id,
        category_id,
        coalesce(category_code, 'unknown') as category_code,
        coalesce(brand, 'unknown') as brand,
        price,
        user_id,
        user_session
*/
    from {{ ref('stg-2019-Oct') }}

),

sessions as (

    select

        user_session,

        min(user_id) as user_id,

        -- representative values for the session
        min(brand) as brand,
        min(category_code) as category_code,

        count(*) as total_activity_count,

        count(*) filter (
            where event_type = 'view'
        ) as view_count,

        count(*) filter (
            where event_type = 'cart'
        ) as cart_add_count,

        count(*) filter (
            where event_type = 'purchase'
        ) as purchase_count,

        max(
            case
                when event_type = 'purchase'
                then 1
                else 0
            end
        ) = 1 as converted,

        min(event_time) as session_start_time,

        max(event_time) as session_end_time,

        datediff(
            'second',
            min(event_time),
            max(event_time)
        ) as session_duration_seconds,

        extract(
            dow from min(event_time)
        ) as day_of_week,

        extract(
            hour from min(event_time)
        ) as hour_of_day,

        min(
            case
                when event_type = 'view'
                then event_time
            end
        ) as first_view_time,

        min(
            case
                when event_type = 'cart'
                then event_time
            end
        ) as first_cart_time

    from cleaned

    group by user_session

)

select

    *,

    datediff(
        'second',
        first_view_time,
        first_cart_time
    ) as seconds_to_first_cart

from sessions