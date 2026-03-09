

SELECT
    locationid::INT     AS zone_key,
    locationid,
    borough,
    zone,
    service_zone
FROM "ny_taxi_db"."bronze"."taxi_zone_lookup"