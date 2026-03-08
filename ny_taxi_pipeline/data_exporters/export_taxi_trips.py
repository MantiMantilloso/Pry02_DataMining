import io
from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def _get_engine():
    """Crea engine de SQLAlchemy usando Secrets de Mage."""
    user     = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host     = "postgres"
    db       = get_secret_value('POSTGRES_DBNAME')
    port     = get_secret_value('POSTGRES_PORT') or '5432'

    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    return create_engine(conn_str)


def _table_exists(connection, schema: str, table: str) -> bool:
    result = connection.execute(text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name   = :table
        );
    """), {'schema': schema, 'table': table})
    return result.scalar()

def _export_trips(engine, trips_df: pd.DataFrame):
    trips_df.columns = trips_df.columns.str.lower()
    raw_conn = engine.raw_connection()
    
    table_name = "trips" # Usamos una sola tabla consolidada

    try:
        with raw_conn.cursor() as cursor:
            # Crear schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

            # Verificar existencia tabla
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'bronze'
                      AND table_name = '{table_name}'
                );
            """)
            exists = cursor.fetchone()[0]

        raw_conn.commit()

        # Si no existe, crearla usando pandas
        if not exists:
            trips_df.head(0).to_sql(
                name=table_name,
                con=engine,
                schema='bronze',
                if_exists='replace',
                index=False
            )
        else:
            # 2. SCHEMA DRIFT HANDLING: Añadir dinámicamente columnas nuevas
            with raw_conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema='bronze' AND table_name='{table_name}'
                """)
                # Postgres guarda los nombres en minúscula por defecto a menos que tengan comillas
                existing_cols = [row[0].lower() for row in cursor.fetchall()]

                for col in trips_df.columns:
                    if col.lower() not in existing_cols:
                        print(f"[SCHEMA DRIFT] Alterando tabla, añadiendo columna: {col}")
                        # Se añade como TEXT en bronze para evitar conflictos de casteo
                        cursor.execute(f'ALTER TABLE bronze.{table_name} ADD COLUMN "{col}" TEXT;')
            raw_conn.commit()

        with raw_conn.cursor() as cursor:
            # Idempotencia requerida por la rúbrica [cite: 119-120]
            source_month = trips_df['source_month'].iloc[0]
            service_type = trips_df['service_type'].iloc[0]
            cursor.execute(f"""
                DELETE FROM bronze.{table_name}
                WHERE source_month = %s
                AND service_type = %s
            """, (source_month, service_type))

            print(f"[IDEMPOTENCIA] DELETE ejecutado para {source_month} - {service_type}")

            # 3. COPY EN CHUNKS (Mapeando columnas explícitamente)
            # Extraemos los nombres exactos de las columnas preservando mayúsculas/minúsculas
            cols_str = '", "'.join(trips_df.columns)
            copy_query = f'COPY bronze.{table_name} ("{cols_str}") FROM STDIN WITH CSV'

            chunk_size = 500000

            for i in range(0, len(trips_df), chunk_size):
                chunk = trips_df.iloc[i:i+chunk_size]

                csv_buffer = io.StringIO()
                chunk.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)

                cursor.copy_expert(copy_query, csv_buffer)

                print(f"[PROGRESS] {min(i+chunk_size, len(trips_df)):,} filas cargadas en {table_name}")

        raw_conn.commit()
        print(f"[OK] {len(trips_df):,} filas insertadas exitosamente con COPY")

    except Exception as e:
        raw_conn.rollback()
        print(f"[ERROR CRÍTICO] Rollback ejecutado: {e}")
        raise e

    finally:
        raw_conn.close()
        
@data_exporter
def export_data_to_postgres(data, **kwargs):
    trips_df = data
    engine = _get_engine()

    print(f"[EXPORT] Insertando {len(trips_df):,} filas con COPY...")
    _export_trips(engine, trips_df)