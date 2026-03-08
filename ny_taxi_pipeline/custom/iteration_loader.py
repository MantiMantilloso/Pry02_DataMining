import pandas as pd
from mage_ai.io.postgres import PostgreSQL
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path
from datetime import datetime
import gc

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def ingest_taxi_data(*args, **kwargs):
    # Mage inyecta dinámicamente las variables del bloque Generador en args[0]
    config = args[0] 
    year = config['year']
    month = config['month']
    service = config['service_type']
    
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
    
    print(f"Descargando: {url}")
    try:
        df = pd.read_parquet(url)
    except Exception as e:
        print(f"Archivo no encontrado o error en descarga: {e}")
        return # Skip elegante si un mes futuro aún no existe
        
    if df.empty:
        return
        
    # Metadatos y control de esquema
    df['ingest_ts'] = datetime.now()
    df['source_month'] = f"{year}-{month:02d}"
    df['service_type'] = service
    df.columns = df.columns.str.lower()
    
    # Conexión nativa de Mage
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    table_name = 'trips'

    with PostgreSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # En lugar del lento DELETE relacional, usamos las funciones de exportación de Mage.
        # Al usar "append", insertamos rápidamente. La idempotencia real la delegaremos a dbt.
        loader.export(
            df,
            schema_name='bronze',
            table_name=table_name,
            index=False,
            if_exists='append', # Agrega a la tabla existente
            allow_reserved_words=True
        )
        
    print(f"[{service}-{year}-{month:02d}] Insertadas {len(df)} filas exitosamente.")
    
    # Liberar memoria agresivamente (crucial en Docker)
    del df
    gc.collect()
    
    return None
