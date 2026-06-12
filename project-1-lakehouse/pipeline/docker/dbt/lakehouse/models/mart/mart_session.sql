-- models/marts/mart_session.sql

{{ config(
    materialized='table',
    database='polaris',
    schema='gold'
) }}

with filtered as (

    select *
    from {{ ref('int_session') }}

    where total_activity_count <= (
        select
            quantile_cont(total_activity_count, 0.999) --reduces total_activity_count (1159 to 69) for top 0.1% sessions, which are likely outliers
        from {{ ref('int_session') }}
    )

)

select *
from filtered