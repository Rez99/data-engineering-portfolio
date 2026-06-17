{{ config(alias='features') }}

with activity_threshold as (

    select percentile(total_activity_count, 0.999) as maximum_activity_count
    from {{ ref('int_sessions') }}

)

select sessions.*
from {{ ref('int_sessions') }} as sessions
cross join activity_threshold
where sessions.total_activity_count
    <= activity_threshold.maximum_activity_count
