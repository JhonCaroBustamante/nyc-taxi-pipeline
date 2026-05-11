%run ./00_setup_and_utils.py

import time
from pyspark.sql import functions as F

start_time = time.time()
logger.info("Iniciando procesamiento de Capa Trusted...")

try:
    df_trips = spark.read.table(f"{CATALOG_NAME}.raw.yellow_trips")
    df_zones = spark.read.table(f"{CATALOG_NAME}.raw.taxi_zones")
    
    # Estandarización de nombres a snake_case
    df_trips = df_trips.withColumnRenamed("PULocationID", "pickup_location_id") \
                       .withColumnRenamed("DOLocationID", "dropoff_location_id")
    
    df_zones = df_zones.withColumnRenamed("LocationID", "location_id") \
                       .withColumnRenamed("Borough", "borough") \
                       .withColumnRenamed("Zone", "zone")

    # Implementación de Reglas de Calidad
    cond_dates = F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")
    cond_dist = (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 500)
    cond_fare = (F.col("fare_amount") > 0) & (F.col("fare_amount") <= 1000)
    
    df_evaluated = df_trips.withColumn("pass_dates", cond_dates) \
                           .withColumn("pass_dist", cond_dist) \
                           .withColumn("pass_fare", cond_fare) \
                           .withColumn("is_valid", cond_dates & cond_dist & cond_fare)
    
    # Guardar reporte de calidad en Refined
    logger.info("Generando reporte de calidad de datos...")
    df_dq_report = df_evaluated.groupBy().agg(
        F.count(F.lit(1)).alias("total_evaluated"),
        F.sum(F.when(~F.col("pass_dates"), 1).otherwise(0)).alias("failed_dates_rule"),
        F.sum(F.when(~F.col("pass_dist"), 1).otherwise(0)).alias("failed_distance_rule"),
        F.sum(F.when(~F.col("pass_fare"), 1).otherwise(0)).alias("failed_fare_rule"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("total_discarded")
    )
    df_dq_report.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.refined.data_quality_report")
    
    dq_row = df_dq_report.collect()[0]
    execution_report["data_quality_summary"] = {
        "total_evaluated": int(dq_row["total_evaluated"]),
        "total_discarded": int(dq_row["total_discarded"]),
        "failed_dates_rule": int(dq_row["failed_dates_rule"]),
        "failed_distance_rule": int(dq_row["failed_distance_rule"]),
        "failed_fare_rule": int(dq_row["failed_fare_rule"])
    }
    
    if dq_row['total_discarded'] > 0:
        logger.warning(f"Calidad de Datos: Se descartaron {dq_row['total_discarded']} registros por no cumplir reglas.")
    else:
        logger.info("Calidad de Datos: 100% de los registros cumplieron las reglas.")
    
    # Filtrar solo los válidos
    df_valid_trips = df_evaluated.filter(F.col("is_valid")).drop("pass_dates", "pass_dist", "pass_fare", "is_valid")
    
    # Join con Taxi Zones
    logger.info("Enriqueciendo datos con el lookup geográfico...")
    df_trusted = df_valid_trips.join(
        df_zones,
        df_valid_trips["pickup_location_id"] == df_zones["location_id"],
        how="left"
    ).drop("location_id") \
     .withColumnRenamed("borough", "pickup_borough") \
     .withColumnRenamed("zone", "pickup_zone")
     
    df_trusted = df_trusted.fillna({"pickup_borough": "Unknown", "pickup_zone": "Unknown"})
    
    # Escritura final en Trusted particionando por fecha
    logger.info(f"Escribiendo tabla particionada en {CATALOG_NAME}.trusted...")
    df_trusted = df_trusted.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df_trusted.write.mode("overwrite").partitionBy("pickup_date").saveAsTable(f"{CATALOG_NAME}.trusted.yellow_trips_enriched")
    
    log_stage_time("Capa Trusted - Procesamiento", start_time)

except Exception as e:
    logger.error(f"Error crítico en la capa Trusted: {str(e)}")
    raise e
