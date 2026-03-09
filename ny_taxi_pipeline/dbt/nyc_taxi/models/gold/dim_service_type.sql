{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY service_type)::INT  AS service_type_key,
    service_type,
    CASE service_type
        WHEN 'yellow' THEN 'Yellow Medallion Taxi'
        WHEN 'green'  THEN 'Green Boro Taxi'
    END AS description
FROM (VALUES ('yellow'), ('green')) AS t(service_type)