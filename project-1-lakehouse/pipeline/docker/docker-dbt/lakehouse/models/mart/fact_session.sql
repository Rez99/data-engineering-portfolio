{{ config(
    materialized='table',
    database='polaris',
    schema='gold'
) }}

SELECT 1 AS id, 'test' AS value