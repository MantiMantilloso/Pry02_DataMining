from mage_ai.data_preparation.decorators import data_loader
from sqlalchemy import create_engine, text
from mage_ai.data_preparation.shared.secrets import get_secret_value

def _get_engine():
    """Crea engine de SQLAlchemy usando Secrets de Mage."""
    user     = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host     = "postgres"
    db       = get_secret_value('POSTGRES_DBNAME')
    port     = get_secret_value('POSTGRES_PORT') or '5432'

    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    return create_engine(conn_str)

@data_loader
def run_partitioning(*args, **kwargs):
    engine = _get_engine()
    with open('/home/src/ny_taxi_pipeline/scripts/create_partitioned_tables.sql') as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("✅ Partitioned tables created")