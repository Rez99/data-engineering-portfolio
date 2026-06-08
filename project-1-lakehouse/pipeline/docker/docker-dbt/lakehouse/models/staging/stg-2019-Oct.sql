{{ config(
    database='polaris',
    schema='bronze'
) }}

select *
from {{ source('ecommerce_clickstream', 'iceberg-2019-Oct') }}