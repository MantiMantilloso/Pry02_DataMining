{{ config(materialized='table', schema='gold') }}

WITH date_spine AS (
    SELECT generate_series(
        '2023-01-01'::date,
        '2025-12-31'::date,
        '1 day'::interval
    )::date AS date_day
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT   AS date_key,
    date_day,
    EXTRACT(YEAR  FROM date_day)::INT    AS year,
    EXTRACT(MONTH FROM date_day)::INT    AS month,
    EXTRACT(DAY   FROM date_day)::INT    AS day,
    EXTRACT(DOW   FROM date_day)::INT    AS day_of_week,
    TO_CHAR(date_day, 'Month')           AS month_name,
    TO_CHAR(date_day, 'Day')             AS day_name,
    EXTRACT(QUARTER FROM date_day)::INT  AS quarter
FROM date_spine