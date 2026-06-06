{{ config(
    materialized='table',
    database='polaris'
) }}

SELECT 1 AS id, 'test' AS value