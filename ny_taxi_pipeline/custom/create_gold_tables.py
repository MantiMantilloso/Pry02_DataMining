if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
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

@custom
def run_partitioning(*args, **kwargs):
    """
    Executes a SQL script to create partitioned tables.
    """
    engine = _get_engine()
    
    # Read the SQL file
    with open('/home/src/ny_taxi_pipeline/scripts/create_partitioned_tables.sql', 'r') as f:
        sql = f.read()
        
    # Use engine.begin() to automatically handle the transaction and commit
    with engine.begin() as conn:
        conn.execute(text(sql))
        
    print("✅ Partitioned tables created successfully")
    
    return {"status": "success", "task": "partitioning_completed"}