from datetime import datetime
import pandas as pd
import gc
import io
from os import path
from sqlalchemy import create_engine
from mage_ai.settings.repo import get_repo_path
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


# ── DB Connection ────────────────────────────────────────────────────────────

def _get_engine():
    user     = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    db       = get_secret_value('POSTGRES_DBNAME')
    port     = get_secret_value('POSTGRES_PORT') or '5432'
    return create_engine(f'postgresql://{user}:{password}@postgres:{port}/{db}')


# ── Idempotent Export ────────────────────────────────────────────────────────

def _upsert_partition(engine, df: pd.DataFrame, schema: str, table: str):
    source_month = df['source_month'].iloc[0]
    service_type = df['service_type'].iloc[0]

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    vendorid              BIGINT,
                    store_and_fwd_flag    TEXT,
                    ratecodeid            BIGINT,
                    pulocationid          BIGINT,
                    dolocationid          BIGINT,
                    passenger_count       BIGINT,
                    trip_distance         DOUBLE PRECISION,
                    fare_amount           DOUBLE PRECISION,
                    extra                 DOUBLE PRECISION,
                    mta_tax               DOUBLE PRECISION,
                    tip_amount            DOUBLE PRECISION,
                    tolls_amount          DOUBLE PRECISION,
                    improvement_surcharge DOUBLE PRECISION,
                    total_amount          DOUBLE PRECISION,
                    payment_type          BIGINT,
                    congestion_surcharge  DOUBLE PRECISION,
                    cbd_congestion_fee    DOUBLE PRECISION,
                    tpep_pickup_datetime  TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    airport_fee           DOUBLE PRECISION,
                    lpep_pickup_datetime  TIMESTAMP,
                    lpep_dropoff_datetime TIMESTAMP,
                    ehail_fee             DOUBLE PRECISION,
                    trip_type             BIGINT,
                    ingest_ts             TIMESTAMP,
                    source_month          TEXT,
                    service_type          TEXT
                );
            """)

            cursor.execute(f"""
                DELETE FROM {schema}.{table}
                WHERE source_month = %s
                  AND service_type = %s
            """, (source_month, service_type))
            deleted_rows = cursor.rowcount
            
            if deleted_rows > 0:
                print(f'🗑️  Borradas {deleted_rows:,} filas existentes para {service_type} {source_month}')
            else:
                print(f'✨  Partición limpia. No hubo necesidad de borrar filas para {service_type} {source_month}')

            cols_str   = '", "'.join(df.columns)
            copy_sql   = f'COPY {schema}.{table} ("{cols_str}") FROM STDIN WITH CSV'
            chunk_size = 500_000

            for i in range(0, len(df), chunk_size):
                buf = io.StringIO()
                df.iloc[i:i + chunk_size].to_csv(buf, index=False, header=False, na_rep='')
                buf.seek(0)
                cursor.copy_expert(copy_sql, buf)
                buf.close()
                print(f'   └─ {min(i + chunk_size, len(df)):,} / {len(df):,} rows copied')
        raw_conn.commit()

    except Exception as e:
        raw_conn.rollback()
        raise RuntimeError(f'Export failed for {service_type} {source_month}: {e}') from e
    finally:
        raw_conn.close()


# ── Main Block ───────────────────────────────────────────────────────────────

@custom
def run(*args, **kwargs):
    START_YEAR = 2022
    END_YEAR   = 2025
    SCHEMA     = 'bronze'
    TABLE      = 'trips'
    SERVICE    = 'green'                         

    table_cols = [
        'vendorid',
        'store_and_fwd_flag',
        'ratecodeid',
        'pulocationid',
        'dolocationid',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'payment_type',
        'congestion_surcharge',
        'cbd_congestion_fee',
        # Yellow-only (null for green)
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'airport_fee',
        # Green-only
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'ehail_fee',
        'trip_type',
        # Metadata
        'ingest_ts',
        'source_month',
        'service_type',
    ]

    rename_map = {
        'vendorid':              'vendorid',
        'lpep_pickup_datetime':  'lpep_pickup_datetime',   # ← changed
        'lpep_dropoff_datetime': 'lpep_dropoff_datetime',  # ← changed
        'passenger_count':       'passenger_count',
        'trip_distance':         'trip_distance',
        'ratecodeid':            'ratecodeid',
        'store_and_fwd_flag':    'store_and_fwd_flag',
        'pulocationid':          'pulocationid',
        'dolocationid':          'dolocationid',
        'payment_type':          'payment_type',
        'fare_amount':           'fare_amount',
        'extra':                 'extra',
        'mta_tax':               'mta_tax',
        'tip_amount':            'tip_amount',
        'tolls_amount':          'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount':          'total_amount',
        'congestion_surcharge':  'congestion_surcharge',
        'ehail_fee':             'ehail_fee',              # ← added
        'trip_type':             'trip_type',              # ← added
        'cbd_congestion_fee':    'cbd_congestion_fee',
        # airport_fee not present in green — will be filled with NA
    }

    int_cols   = ['vendorid', 'passenger_count', 'ratecodeid',
                  'pulocationid', 'dolocationid', 'payment_type',
                  'trip_type']                             # ← added trip_type
    float_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax',
                  'tip_amount', 'tolls_amount', 'improvement_surcharge',
                  'total_amount', 'congestion_surcharge', 'ehail_fee',  # ← added ehail_fee
                  'cbd_congestion_fee']
                  # airport_fee removed — not in green
    dt_cols    = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']      # ← changed

    def to_table_schema(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
        df.columns = [c.lower() for c in df.columns]

        for src in rename_map:
            if src not in df.columns:
                df[src] = pd.NA

        df = df.rename(columns=rename_map)

        for col in table_cols:
            if col not in df.columns:
                df[col] = pd.NA

        for c in dt_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors='coerce')
        for c in int_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
        for c in float_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float64')

        df['ingest_ts']    = datetime.now()
        df['source_month'] = f'{year}-{month:02d}'
        df['service_type'] = SERVICE

        return df[table_cols]

    total_rows = 0
    skipped    = []
    months     = [f'{m:02d}' for m in range(1, 13)]
    engine     = _get_engine()

    for year in range(START_YEAR, END_YEAR + 1):
        for month in months:
            url = (
                f'https://d37ci6vzurychx.cloudfront.net/trip-data/'
                f'green_tripdata_{year}-{month}.parquet'  # ← changed
            )
            print(f'📥  [{SERVICE}]  {year}-{month}  →  {url}')

            try:
                df = pd.read_parquet(url)
            except Exception as e:
                msg = f'[{SERVICE}] {year}-{month} | download skipped: {e}'
                print(f'⚠️  {msg}')
                skipped.append(msg)
                continue

            try:
                df = to_table_schema(df, year, int(month))
            except Exception as e:
                msg = f'[{SERVICE}] {year}-{month} | schema error: {e}'
                print(f'❌  {msg}')
                skipped.append(msg)
                del df; gc.collect()
                continue

            try:
                _upsert_partition(engine, df, SCHEMA, TABLE)
                rows = len(df)
                total_rows += rows
                print(f'✅  [{SERVICE}]  {year}-{month}: {rows:,} rows')
            except Exception as e:
                msg = f'[{SERVICE}] {year}-{month} | export error: {e}'
                print(f'❌  {msg}')
                skipped.append(msg)
            finally:
                del df
                gc.collect()

    print(f'\n{"=" * 60}')
    print(f'Total rows loaded : {total_rows:,}')
    print(f'Skipped partitions: {len(skipped)}')
    for s in skipped:
        print(f'   - {s}')
    print(f'{"=" * 60}\n')
    return f'OK: {total_rows:,} rows  |  skipped: {len(skipped)}'