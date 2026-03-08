import io
from datetime import datetime
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # Recuperar variables de ejecución
    year = int(kwargs['year'])
    month = int(kwargs['month'])
    service = kwargs['service_type']
    
    # Construir URL dinámica
    trips_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
    # Para este ejemplo deberia devolver: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    print(f"Descargando trips datos desde: {trips_url}")
    
    df_trips = pd.read_parquet(trips_url)

    # Metadatos obligatorios
    df_trips['ingest_ts']    = datetime.now()
    df_trips['source_month'] = f"{year}-{month:02d}"
    df_trips['service_type'] = service

    return df_trips