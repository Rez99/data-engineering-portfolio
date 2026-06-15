select 1 as integration_ok
from {{ source('ecommerce_clickstream', 'events') }}
limit 1
