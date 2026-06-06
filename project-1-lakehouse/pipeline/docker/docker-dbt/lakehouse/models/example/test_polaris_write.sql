{{ config(
    materialized='table',
    database='polaris',
    schema='goldd'
) }}

SELECT 1 AS id, 'test' AS value