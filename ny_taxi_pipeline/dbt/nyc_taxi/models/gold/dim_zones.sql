{{ config(materialized='table', schema='gold') }}

SELECT
    locationid::INT     AS zone_key,
    locationid,
    borough,
    zone,
    service_zone
FROM {{ source('bronze', 'taxi_zone_lookup') }}