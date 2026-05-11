import logging
import time
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Configuración de Observabilidad (Logs)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NycTaxiPipeline")

# Diccionario global para el reporte de ejecución final en JSON
execution_report = {
    "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "stages": {},
    "data_quality_summary": {},
    "kpis_summary": {}
}

def log_stage_time(stage_name, start_time):
    elapsed = time.time() - start_time
    execution_report["stages"][stage_name] = f"{elapsed:.2f} seconds"
    logger.info(f"ETAPA COMPLETADA: {stage_name} en {elapsed:.2f}s")

# 2. Configuración de Unity Catalog
CATALOG_NAME = "nyc_taxi_jhon_caro"

try:
    logger.info("Configurando Unity Catalog y Esquemas...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"USE CATALOG {CATALOG_NAME}")
    
    schemas = ["raw", "trusted", "refined"]
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
    logger.info(f"Catálogo '{CATALOG_NAME}' y esquemas creados exitosamente.")
except Exception as e:
    logger.error(f"Error configurando Unity Catalog: {str(e)}")
    raise e
