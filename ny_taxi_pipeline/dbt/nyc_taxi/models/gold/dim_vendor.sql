{{ config(materialized='table', schema='gold') }}

SELECT
    vendorid::INT   AS vendor_key,
    CASE vendorid
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc.'
        ELSE        'Unknown'
    END             AS vendor_name
FROM (VALUES (1), (2)) AS t(vendorid)