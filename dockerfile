FROM mageai/mageai

# Instalar dbt-postgres y cualquier otra dependencia necesaria
# Se incluye dbt-core automáticamente al instalar el adaptador
RUN pip install dbt-postgres

# Copiar requirements.txt si tienes otras librerías (ej. pandas, sqlalchemy)
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt