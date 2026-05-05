import os
import sys
import traceback
import datetime # <-- Agregado para leer los formatos de fecha
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

print("Iniciando script de migración...")

try:
    # 1. Configuración de conexión a Supabase
    DB_USER = "postgres.zrqkrtkyzaeywvimgnqk"
    DB_PASS = os.getenv("SUPABASE_PASSWORD")
    DB_HOST = "aws-1-us-east-1.pooler.supabase.com"
    DB_PORT = "6543"
    DB_NAME = "postgres"

    conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)

    # 2. Configuración de BigQuery
    PROJECT_ID = "rock-hangar-470622-u5"
    DATASET_ID = "conjunto_datos_propio"
    client = bigquery.Client(project=PROJECT_ID)

    # 3. DICCIONARIO DE TABLAS
    tablas_info = {
        "job_offers_linkedin": {"id_col": "id", "fecha_col": "created_at"},
        "ofertas_empleo": {"id_col": "id", "fecha_col": "fecha_hora_publicacion"},
        "ofertas_historial": {"id_col": "historial_id", "fecha_col": "fecha_hora_publicacion"}
    }

    errores_totales = 0
    print("Iniciando carga INCREMENTAL inteligente (Ventana de 3 días)...")

    for tabla, config in tablas_info.items():
        col_id = config["id_col"]
        col_fecha = config["fecha_col"]
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{tabla}"
        
        print(f"\n--- Procesando tabla: {tabla} ---")
        
        try:
            # PASO 1: Extraer
            query_supa = f"""
                SELECT * FROM {tabla} 
                WHERE {col_fecha} >= (CURRENT_DATE - INTERVAL '365 days')
            """
            df_nuevos = pd.read_sql_query(query_supa, engine)
            print(f"[{tabla}] Registros encontrados en Supabase (últimos 3 días): {len(df_nuevos)}")

            if df_nuevos.empty:
                print(f"[{tabla}] No hay datos nuevos en este periodo. Saltando...")
                continue

            # PASO 1.5: Limpieza de tipos de datos para PyArrow (EL FIX)
            # Esto busca columnas que Pandas guardó como texto, pero que por dentro tienen fechas
            for col in df_nuevos.columns:
                if df_nuevos[col].dtype == 'object':
                    valid_data = df_nuevos[col].dropna()
                    # Si el primer dato válido de la columna es una fecha, convierte toda la columna
                    if not valid_data.empty and isinstance(valid_data.iloc[0], datetime.date):
                        df_nuevos[col] = pd.to_datetime(df_nuevos[col])

            # PASO 2: Verificar IDs
            try:
                query_bq = f"SELECT {col_id} FROM `{table_id}`"
                df_bq = client.query(query_bq).to_dataframe()
                ids_en_bq = set(df_bq[col_id].tolist())
                print(f"[{tabla}] Total de IDs históricos en BigQuery: {len(ids_en_bq)}")
            except Exception:
                print(f"[{tabla}] La tabla no existe en BQ. Se creará con la primera carga.")
                ids_en_bq = set()

            # PASO 3: Filtrar
            df_final = df_nuevos[~df_nuevos[col_id].isin(ids_en_bq)]
            print(f"[{tabla}] Registros nuevos para insertar: {len(df_final)}")

            # PASO 4: Insertar
            if not df_final.empty:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                client.load_table_from_dataframe(df_final, table_id, job_config=job_config).result()
                print(f"[{tabla}] ✅ Actualización finalizada con éxito.")
            else:
                print(f"[{tabla}] ⚡ Sin cambios necesarios. Todos los IDs ya existen.")

        except Exception as e:
            errores_totales += 1
            print(f"\n❌ ERROR EN LA TABLA {tabla}:")
            print(traceback.format_exc())
            print("-" * 50)

    print("\n--- Sincronización terminada ---")

    if errores_totales > 0:
        print(f"\n⚠️ El proceso finalizó, pero se encontraron {errores_totales} errores. Revisa el log de arriba.")
        sys.exit(1)

except Exception as e:
    print("\n🔥 ERROR FATAL: Falló la conexión inicial a la base de datos o BigQuery.")
    print(traceback.format_exc())
    sys.exit(1)
