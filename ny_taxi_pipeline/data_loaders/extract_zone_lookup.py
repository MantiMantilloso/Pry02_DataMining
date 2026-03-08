import pandas as pd
from datetime import datetime
from mage_ai.io.file import FileIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    zone_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    print(f"Descargando desde: {zone_url}")

    df = pd.read_csv(zone_url)

    # Normalizar columnas
    df.columns = [c.lower() for c in df.columns]

    # Metadatos
    df['ingest_ts'] = datetime.now()

    print(f"[OK] Zone Lookup → {len(df):,} filas")
    print(f"[COLUMNAS] {list(df.columns)}")
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
