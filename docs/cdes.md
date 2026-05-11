# Elementos Críticos de Datos (CDEs) - NYC Yellow Taxi

Este documento identifica los 3 Critical Data Elements (CDEs) fundamentales del dataset para garantizar la validez, integridad y auditabilidad del modelo de negocio analítico.

| Nombre del CDE | Tabla / Columna donde vive | Definición de Negocio | Regla de Calidad Aplicada |
| :--- | :--- | :--- | :--- |
| **Fechas del Viaje** | `raw.yellow_trips` <br> `tpep_pickup_datetime` <br> `tpep_dropoff_datetime` | Marcas de tiempo exactas en las que el pasajero inicia el servicio y llega a su destino. Permiten calcular duraciones y segmentar franjas de demanda temporal. | La fecha y hora de inicio debe ser estrictamente anterior a la de finalización (`pickup < dropoff`). |
| **Distancia del Viaje** | `raw.yellow_trips` <br> `trip_distance` | Distancia total recorrida en millas reportada por el taxímetro del vehículo. Es una métrica base para evaluar la velocidad y la eficiencia económica por zona. | Debe ser mayor a 0 y menor o igual a 500 millas para aislar errores de hardware o digitación (`0 < distance <= 500`). |
| **Tarifa del Viaje** | `raw.yellow_trips` <br> `fare_amount` | Costo base del viaje cobrado al pasajero (excluyendo propinas y peajes). Representa el ingreso directo para los análisis de rentabilidad de la flota. | Debe ser mayor a $0 y menor o igual a $1000 frente a umbrales geográficos lógicos de la ciudad (`0 < fare <= 1000`). |