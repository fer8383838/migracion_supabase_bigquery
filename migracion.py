import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

# 1. Configuración de conexión a Supabase
DB_USER = "postgres.zrqkrtkyzaeywvimgnqk"
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_HOST = "aws-1-us-east-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

# Conexión SQLAlchemy
conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

# 2. Configuración de BigQuery
PROJECT_ID = "rock-hangar-470622-u5"
DATASET_ID = "conjunto_datos_propio"
client = bigquery.Client(project=PROJECT_ID)

# 3. Lista de tablas a migrar (CORREGIDA)
tablas_a_migrar = ["job_offers_linkedin", "ofertas_empleo", "ofertas_historial"]

print("Iniciando migración masiva...")

for tabla in tablas_a_migrar:
    try:
        print(f"--- Procesando tabla: {tabla} ---")
        
        # Extracción de Supabase
        print(f"Extrayendo datos de {tabla}...")
        df = pd.read_sql_table(tabla, engine)
        print(f"Filas obtenidas: {len(df)}")

        # Configuración de destino
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{tabla}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        # Carga a BigQuery
        print(f"Cargando en BigQuery: {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        print(f"Éxito: Tabla {tabla} actualizada.")
        
    except Exception as e:
        print(f"ERROR en tabla {tabla}: {e}")

print("--- Proceso finalizado para todas las tablas ---")
