# Glosario de Negocio

Definición del lenguaje ubicuo utilizado entre los equipos de ingeniería, gobierno de datos y analítica de negocio.

1. **Viaje Válido**: Registro transaccional que ha superado exitosamente todas las expectativas de calidad en la capa Trusted (fechas lógicas congruentes, distancia y tarifa mayores a cero, y ubicados dentro de los umbrales de outliers documentados).
2. **Franja Horaria de Demanda**: Segmentación del día en 6 intervalos de 4 horas cada uno (ej. *Madrugada*, *Mañana Temprana*, etc.) utilizada para aislar y entender los patrones temporales de movilidad de los usuarios.
3. **Hora Pico (Pico de Demanda net)**: El bloque temporal específico (combinación de día de la semana y franja horaria) que concentra el mayor volumen neto de viajes iniciados.
4. **Borough**: División administrativa principal de la ciudad de Nueva York (Manhattan, Brooklyn, Queens, Bronx, Staten Island) a la que pertenece una zona de recogida específica.
5. **Eficiencia Económica (Ingreso por Milla)**: Métrica de rentabilidad financiera que evalúa el valor monetario generado por cada milla recorrida, optimizando la identificación de las áreas geográficas más lucrativas (`total_amount / trip_distance`).