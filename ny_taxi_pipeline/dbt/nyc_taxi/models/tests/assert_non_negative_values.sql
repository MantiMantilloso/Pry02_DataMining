SELECT *
FROM {{ ref('silver_trips') }}
WHERE trip_distance < 0
   OR total_amount < 0