from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text

# Conectamos usando tus Mage Secrets
user     = get_secret_value('POSTGRES_USER')
password = get_secret_value('POSTGRES_PASSWORD')
db       = get_secret_value('POSTGRES_DBNAME')
port     = get_secret_value('POSTGRES_PORT') or '5432'

engine = create_engine(f'postgresql://{user}:{password}@postgres:{port}/{db}')

with engine.begin() as conn:
    conn.execute(text("DROP SCHEMA IF EXISTS bronze CASCADE;"))
    print("[OK] Esquema Bronze destruido con éxito. ¡Limpieza terminada!")