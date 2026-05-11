%run ./00_setup_and_utils.py

import time
import json
from pyspark.sql import functions as F

if 'logger' not in locals() and 'logger' not in globals():
    raise NameError("Contexto no inicializado. Ejecuta primero la Celda 1 con el comando %run.")

start_time = time.time()
logger.info("Iniciando motor de agregación analítica en Capa Refined (Gold)...")

try:
    df = spark.read.table(f"{CATALOG_NAME}.trusted.yellow_trips_enriched")
    
    # Cálculo transaccional de duración compatible con ANSI SQL
    df = df.withColumn(
        "duration_minutes", 
        (F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))) / 60.0
    )
    
    # ==========================================================================
    # KPI 1: PATRÓN DE DEMANDA TEMPORAL (6 FRANJAS Y DÍA DE SEMANA)
    # ==========================================================================
    logger.info("Calculando KPI 1: Patrones de demanda temporal...")
    df = df.withColumn("hour", F.hour("tpep_pickup_datetime")) \
           .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
           
    df = df.withColumn(
        "time_slot",
        F.when((F.col("hour") >= 0) & (F.col("hour") < 4), "1. Madrugada (00-03)")
         .when((F.col("hour") >= 4) & (F.col("hour") < 8), "2. Mañana Temprana (04-07)")
         .when((F.col("hour") >= 8) & (F.col("hour") < 12), "3. Mañana (08-11)")
         .when((F.col("hour") >= 12) & (F.col("hour") < 16), "4. Tarde (12-15)")
         .when((F.col("hour") >= 16) & (F.col("hour") < 20), "5. Tarde-Noche (16-19)")
         .otherwise("6. Noche (20-23)")
    )
    
    kpi1_demand = df.groupBy("day_of_week", "time_slot").agg(
        F.count(F.lit(1)).alias("total_trips"),
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.avg("fare_amount").alias("avg_fare_amount")
    ).orderBy("day_of_week", "time_slot")
    
    kpi1_demand.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.refined.kpi1_temporal_demand")
    
    if 'enforce_delta_optimizations' in globals():
        enforce_delta_optimizations(f"{CATALOG_NAME}.refined.kpi1_temporal_demand")
    
    # Captura del pico de demanda para la observabilidad
    peak_slot = kpi1_demand.orderBy(F.col("total_trips").desc()).first()
    execution_report["kpis_summary"]["peak_demand_slot"] = f"{peak_slot['day_of_week']} - {peak_slot['time_slot']} ({peak_slot['total_trips']} viajes)"

    # ==========================================================================
    # KPI 2: EFICIENCIA ECONÓMICA POR ZONA (TOP 10 RENTABLES)
    # ==========================================================================
    logger.info("Calculando KPI 2: Eficiencia económica y velocidades geográficas...")
    df_speed = df.filter(F.col("duration_minutes") > 0).withColumn(
        "speed_mph", F.col("trip_distance") / (F.col("duration_minutes") / 60.0)
    ).withColumn(
        "revenue_per_mile", F.col("total_amount") / F.col("trip_distance")
    )
    
    kpi2_efficiency = df_speed.groupBy("pickup_borough", "pickup_zone").agg(
        F.avg("revenue_per_mile").alias("avg_revenue_per_mile"),
        F.avg("speed_mph").alias("avg_speed_mph"),
        F.sum("total_amount").alias("total_revenue")
    ).orderBy(F.col("avg_revenue_per_mile").desc()).limit(10)
    
    kpi2_efficiency.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.refined.kpi2_zone_efficiency_top10")
    
    if 'enforce_delta_optimizations' in globals():
        enforce_delta_optimizations(f"{CATALOG_NAME}.refined.kpi2_zone_efficiency_top10")

    # ==========================================================================
    # KPI 3: IMPACTO FINANCIERO DE LA CALIDAD DE LOS DATOS (COLUMNAR PRUNED)
    # ==========================================================================
    logger.info("Calculando KPI 3: Auditoría de impacto en ingresos (I/O Pruned)...")
    # Lectura columnar estricta para no saturar el I/O del clúster
    df_raw_pruned = spark.read.table(f"{CATALOG_NAME}.raw.yellow_trips").select("total_amount")
    
    total_revenue_raw = df_raw_pruned.agg(F.sum("total_amount")).collect()[0][0] or 0.0
    total_revenue_trusted = df.agg(F.sum("total_amount")).collect()[0][0] or 0.0
    revenue_lost = total_revenue_raw - total_revenue_trusted
    
    execution_report["kpis_summary"]["data_quality_impact"] = {
        "revenue_raw_dataset": round(float(total_revenue_raw), 2),
        "revenue_trusted_dataset": round(float(total_revenue_trusted), 2),
        "revenue_discarded_due_to_rules": round(float(revenue_lost), 2)
    }
    
    log_stage_time("Capa Refined - KPIs Calculados", start_time)
    
    # ==========================================================================
    # EMISIÓN DEL REPORTE FINAL DE OBSERVABILIDAD EN JSON
    # ==========================================================================
    logger.info("PIPELINE MEDALLION EJECUTADO EXITOSAMENTE. Emitiendo JSON final:")
    print("\n" + "="*60)
    print(" REPORTE FINAL DE EJECUCIÓN (JSON DE OBSERVABILIDAD) ")
    print("="*60)
    print(json.dumps(execution_report, indent=4))

except Exception as e:
    logger.error(f"Fallo crítico calculando los agregados de la Capa Gold: {str(e)}")
    raise e
