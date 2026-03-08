from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def _get_engine():
    user     = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host     = 'postgres'
    db       = get_secret_value('POSTGRES_DBNAME')
    port     = get_secret_value('POSTGRES_PORT') or '5432'

    print(f"[DEBUG] Conectando a {host}:{port}/{db} como {user}")
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


@data_exporter
def export_zone_lookup(df: pd.DataFrame, **kwargs) -> None:
    engine = _get_engine()

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

            df.to_sql(
                name='taxi_zone_lookup',
                con=connection,
                schema='bronze',
                if_exists='replace',   # Siempre reemplazar, es referencia estática
                index=False,
            )

            trans.commit()
            print(f"[OK] bronze.taxi_zone_lookup → {len(df):,} filas cargadas")

        except Exception as e:
            trans.rollback()
            print(f"[ERROR] Rollback ejecutado: {e}")
            raise e