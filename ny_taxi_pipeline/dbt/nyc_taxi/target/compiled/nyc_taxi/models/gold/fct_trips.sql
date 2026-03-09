

SELECT
    pickup_ts::DATE   AS pickup_date,
    pickup_ts,
    dropoff_ts,
    s.service_type    AS service_type_key,
    s.vendorid        AS vendor_key,
    CASE s.payment_type
        WHEN 1 THEN 'card'
        WHEN 2 THEN 'cash'
        WHEN 3 THEN 'other'
        ELSE 'unknown'
    END               AS payment_type_key,
    s.pulocationid    AS pu_zone_key,
    s.dolocationid    AS do_zone_key,
    s.passenger_count,
    s.trip_distance,
    s.fare_amount,
    s.extra,
    s.mta_tax,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.congestion_surcharge,
    s.cbd_congestion_fee,
    s.airport_fee,
    s.total_amount,
    s.source_month
FROM "ny_taxi_db"."silver"."silver_trips" s