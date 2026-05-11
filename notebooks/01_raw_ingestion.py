%run ./00_setup_and_utils.py
import urllib.request
import time

start_time = time.time()
logger.info("Iniciando Ingesta de Capa Raw mediante UC Volumes...")

# URLs de origen
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

try:
    # 1. Crear un Volumen gobernado en Unity Catalog para los archivos Raw
    volume_name = "raw_files"
    logger.info(f"Configurando Volumen de Unity Catalog: {CATALOG_NAME}.raw.{volume_name}...")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.raw.{volume_name}")
    
    # La ruta del volumen mapeada de forma segura
    volume_path = f"/Volumes/{CATALOG_NAME}/raw/{volume_name}"
    
    local_parquet = f"{volume_path}/yellow_tripdata_2023-01.parquet"
    local_csv = f"{volume_path}/taxi_zone_lookup.csv"
    
    # 2. Descarga de datos directamente al Volumen gobernado
    logger.info("Descargando archivos desde fuentes públicas al Volumen...")
    urllib.request.urlretrieve(parquet_url, local_parquet)
    urllib.request.urlretrieve(csv_url, local_csv)
    
    # 3. Lectura y tipado básico (leyendo nativamente desde el Volumen)
    logger.info("Leyendo archivos desde el Volumen con PySpark...")
    df_trips_raw = spark.read.parquet(local_parquet)
    df_zones_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(local_csv)
    
    # 4. Escritura en tablas Delta administradas por Unity Catalog (Capa Raw)
    logger.info(f"Escribiendo tablas Delta en: {CATALOG_NAME}.raw...")
    df_trips_raw.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.raw.yellow_trips")
    df_zones_raw.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.raw.taxi_zones")
    
    # 5. Registro de métricas de observabilidad
    count_trips = df_trips_raw.count()
    count_zones = df_zones_raw.count()
    
    logger.info(f"Ingesta Raw completada exitosamente. Registros viajes: {count_trips}, Registros zonas: {count_zones}")
    execution_report["total_records_read"] = count_trips
    
    log_stage_time("Capa Raw - Ingesta", start_time)

except Exception as e:
    logger.error(f"Fallo crítico en la ingesta Raw: {str(e)}")
    raise e
