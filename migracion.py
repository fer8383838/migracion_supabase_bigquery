import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

# 1. Configuración de conexión a Supabase (Tus credenciales exactas)
DB_USER = "postgres.zrqkrtkyzaeywvimgnqk"
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_HOST = "aws-1-us-east-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

# Conexión usando SQLAlchemy para que Pandas lo lea perfecto
conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

print("Conectando a Supabase...")
# Extraemos tu tabla
df = pd.read_sql_table("job_offers_linkedin", engine)
print(f"Datos extraídos: {len(df)} filas.")

# 2. Configuración de BigQuery
PROJECT_ID = "rock-hangar-470622-u5"
DATASET_ID = "conjunto_datos_propio"
TABLE_NAME = "job_offers_linkedin"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

client = bigquery.Client(project=PROJECT_ID)

# 3. Carga de datos
print(f"Cargando datos en BigQuery ({TABLE_ID})...")

# WRITE_TRUNCATE: Si la tabla no existe, la crea. Si existe, la sobreescribe con los datos del día.
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Subimos los datos a BigQuery
job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
job.result() # Espera a que termine la subida

print("¡Listo! Tabla creada y actualizada en BigQuery.")
