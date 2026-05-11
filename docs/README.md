# NYC Yellow Taxi Pipeline - Medallion Architecture & Data Governance

Este repositorio contiene la solución técnica para el procesamiento, gobierno de datos, observabilidad y analítica del dataset público de taxis amarillos de Nueva York (Enero 2023). El pipeline está implementado sobre **Azure Databricks** mediante **PySpark** y estructurado bajo el gobierno transaccional de **Unity Catalog**.

## 🚀 Guía de Reproducción y Ejecución

1. **Prerrequisitos**: Disponer de un Workspace de Databricks con permisos habilitados para la creación de catálogos, esquemas y volúmenes en Unity Catalog.
2. **Despliegue del Código**: Importa los 4 scripts de la carpeta `notebooks/` en tu entorno de Databricks, asegurándote de mantenerlos agrupados en el mismo directorio.
3. **Ejecución Secuencial**:
   * **`00_setup_and_utils.py`**: Módulo transversal que inicializa el catálogo (`nyc_taxi_jhon_caro`), los esquemas (`raw`, `trusted`, `refined`), inyecta propiedades globales de optimización física Delta y expone el framework de observabilidad.
   * **`01_raw_ingestion.py`**: Capa Bronze. Aísla la descarga física de las fuentes externas enrutándolas hacia un **Unity Catalog Volume** seguro (`/Volumes/...`), define contratos de datos estrictos para evitar inferencias costosas en red y persiste las tablas Delta base.
   * **`02_trusted_processing.py`**: Capa Silver. Ejecuta la limpieza de datos, evalúa expectativas de calidad de forma no destructiva (persistiendo la auditoría en `refined.data_quality_report`) y enriquece la información mediante un **Broadcast Join** en memoria con el lookup geográfico.
   * **`03_refined_kpis.py`**: Capa Gold. Procesa los agregados analíticos de negocio (KPIs 1 y 2). Adicionalmente, implementa el **KPI 3 (Opcional)** aplicando *Columnar Pruning* sobre la capa Raw para evaluar el impacto financiero exacto derivado de la calidad de los datos, emitiendo finalmente el **Reporte Final de Observabilidad en JSON** hacia la salida estándar.

## 🧠 Decisiones Técnicas Clave y Optimizaciones

* **Ingesta Segura mediante UC Volumes**: Dado que Unity Catalog en *Shared Mode* bloquea por diseño de seguridad las escrituras locales hacia `/tmp` o `/dbfs`, se implementó un Volumen gobernado. Esto permite descargar los archivos físicos nativamente desde internet, manteniendo todo su ciclo de vida y linaje estrictamente auditado.
* **Auditoría de Calidad Transparente**: En lugar de aplicar un filtro destructivo estándar que elimine datos atípicos, se procesaron banderas lógicas de validación. Esto garantizó la persistencia transaccional de la tabla de auditoría `refined.data_quality_report` y permitió aislar financieramente el costo de la mala calidad en el origen (KPI 3).
* **Estrategia de Cruce (Broadcast Join)**: Al cruzar la tabla de viajes (millones de registros) con el lookup de zonas (~265 registros), se forzó un *Broadcast Join*. Al enviar una copia de la tabla pequeña a la memoria de cada *executor*, se eliminó el 100% del *shuffle* de red de la tabla masiva, garantizando una complejidad O(1) en la transferencia.
* **Compatibilidad Estricta ANSI SQL**: Para el cálculo de duraciones, se reemplazó el casteo directo a enteros por la API nativa `unix_timestamp()`, evitando fallos de *DataType Mismatch* y cumpliendo con los estándares de cumplimiento ANSI SQL activados por defecto en Databricks Runtime 13+.
* **Estrategia de Particionado Física**: La tabla Trusted se particionó por `pickup_date`. Al ser consultas orientadas a series temporales, el motor aprovecha el *partition pruning* para omitir la lectura de carpetas irrelevantes en disco, acelerando drásticamente las agrupaciones.
* **Imputación Semántica de Nulos**: En el enriquecimiento geográfico, los valores sin cruce se imputaron explícitamente como `"Unknown"` para asegurar que los reportes globales de rentabilidad no sufran pérdidas de ingresos agregados por omisión de nulos.

## 📊 Linaje de Datos (Lineage)

El linaje nativo capturado automáticamente por Unity Catalog evidencia el flujo de datos y dependencias de la arquitectura Medallion:
*(Ver captura visual en `docs/linaje.png`)*.

## ⚠️ Limitaciones Conocidas

* **Delta Live Tables (DLT)**: Debido a las restricciones de concurrencia y licenciamiento en los entornos *Community/Free Trial*, el motor de expectativas (*Expectations*) se implementó mediante un framework programático y altamente optimizado en PySpark nativo, cumpliendo con exactitud el objetivo de auditoría y aislamiento de registros.