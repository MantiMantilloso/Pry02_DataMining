{{ config(
    materialized='view',
    schema='silver'
) }}

WITH base AS (
    SELECT
        -- Unified timestamps
        CASE
            WHEN service_type = 'yellow' THEN tpep_pickup_datetime
            WHEN service_type = 'green'  THEN lpep_pickup_datetime
        END AS pickup_ts,

        CASE
            WHEN service_type = 'yellow' THEN tpep_dropoff_datetime
            WHEN service_type = 'green'  THEN lpep_dropoff_datetime
        END AS dropoff_ts,

        -- Core fields
        service_type,
        vendorid,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        cbd_congestion_fee,
        airport_fee,
        ehail_fee,
        trip_type,
        total_amount,
        payment_type,
        source_month,
        ingest_ts

    FROM {{ source('bronze', 'trips') }}
),

quality_filtered AS (
    SELECT *
    FROM base
    WHERE
        pickup_ts IS NOT NULL
        AND dropoff_ts IS NOT NULL
        AND pickup_ts <= dropoff_ts
        AND trip_distance >= 0
        AND total_amount >= 0
        AND pulocationid IS NOT NULL
        AND dolocationid IS NOT NULL
)
SELECT * FROM quality_filtered