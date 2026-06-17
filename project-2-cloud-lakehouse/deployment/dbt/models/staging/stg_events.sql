select
    event_time,
    event_type,
    product_id,
    category_id,
    coalesce(category_code, 'unknown') as category_code,
    coalesce(brand, 'unknown') as brand,
    price,
    user_id,
    user_session
from {{ source('ecommerce_clickstream', 'events') }}
where user_session is not null
