-- ─────────────────────────────────────────────
-- FACT: fct_trips — RANGE by pickup_date
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.fct_trips CASCADE;
CREATE TABLE gold.fct_trips (
    trip_id             BIGSERIAL,
    pickup_date         DATE        NOT NULL,
    pickup_ts           TIMESTAMP,
    dropoff_ts          TIMESTAMP,
    service_type_key    INT,
    vendor_key          INT,
    payment_type_key    INT,
    pu_zone_key         INT,
    do_zone_key         INT,
    passenger_count     BIGINT,
    trip_distance       DOUBLE PRECISION,
    fare_amount         DOUBLE PRECISION,
    extra               DOUBLE PRECISION,
    mta_tax             DOUBLE PRECISION,
    tip_amount          DOUBLE PRECISION,
    tolls_amount        DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    cbd_congestion_fee    DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    source_month          TEXT
) PARTITION BY RANGE (pickup_date);

-- Monthly partitions Q1 2023 → Q4 2025
DO $$
DECLARE
    y INT;
    m INT;
    start_date DATE;
    end_date   DATE;
    part_name  TEXT;
BEGIN
    FOR y IN 2023..2025 LOOP
        FOR m IN 1..12 LOOP
            start_date := make_date(y, m, 1);
            end_date   := start_date + INTERVAL '1 month';
            part_name  := format('fct_trips_%s_%s', y, lpad(m::text, 2, '0'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS gold.%I
                 PARTITION OF gold.fct_trips
                 FOR VALUES FROM (%L) TO (%L)',
                part_name, start_date, end_date
            );
        END LOOP;
    END LOOP;
END $$;


-- ─────────────────────────────────────────────
-- DIM: dim_zone — HASH (4 partitions)
-- ─────────────────────────────────────────────
DROP TABLE IF EXISTS gold.dim_zone CASCADE;
CREATE TABLE gold.dim_zone (
    zone_key        INT         NOT NULL,
    locationid      BIGINT,
    borough         TEXT,
    zone            TEXT,
    service_zone    TEXT
) PARTITION BY HASH (zone_key);

CREATE TABLE gold.dim_zone_p0 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE gold.dim_zone_p1 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE gold.dim_zone_p2 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE gold.dim_zone_p3 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 3);


-- ─────────────────────────────────────────────
-- DIM: dim_service_type — LIST
-- ─────────────────────────────────────────────
DROP TABLE IF EXISTS gold.dim_service_type CASCADE;
CREATE TABLE gold.dim_service_type (
    service_type_key    INT         NOT NULL,
    service_type        TEXT        NOT NULL,
    description         TEXT
) PARTITION BY LIST (service_type);

CREATE TABLE gold.dim_service_type_yellow
    PARTITION OF gold.dim_service_type FOR VALUES IN ('yellow');
CREATE TABLE gold.dim_service_type_green
    PARTITION OF gold.dim_service_type FOR VALUES IN ('green');


-- ─────────────────────────────────────────────
-- DIM: dim_payment_type — LIST
-- ─────────────────────────────────────────────
DROP TABLE IF EXISTS gold.dim_payment_type CASCADE;
CREATE TABLE gold.dim_payment_type (
    payment_type_key    INT         NOT NULL,
    payment_type        TEXT        NOT NULL,
    description         TEXT
) PARTITION BY LIST (payment_type);

CREATE TABLE gold.dim_payment_type_card
    PARTITION OF gold.dim_payment_type FOR VALUES IN ('card');
CREATE TABLE gold.dim_payment_type_cash
    PARTITION OF gold.dim_payment_type FOR VALUES IN ('cash');
CREATE TABLE gold.dim_payment_type_other
    PARTITION OF gold.dim_payment_type FOR VALUES IN ('other', 'unknown');


-- ─────────────────────────────────────────────
-- DIM: dim_date — no partitioning needed
-- DIM: dim_vendor — no partitioning needed
-- ─────────────────────────────────────────────
DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.dim_vendor CASCADE;