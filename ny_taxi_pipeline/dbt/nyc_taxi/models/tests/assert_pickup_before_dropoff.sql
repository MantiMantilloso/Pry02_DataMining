SELECT *
FROM {{ ref('silver_trips') }}
WHERE pickup_ts > dropoff_ts