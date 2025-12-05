# Databricks notebook source
# MAGIC %md
# MAGIC # Usandos los datos nyctaxi.trips

# COMMAND ----------

from pyspark.sql.functions import col, floor, hour, year, month, concat_ws, desc, row_number, concat_ws
from pyspark.sql.functions import avg as _avg, count as _count
from pyspark.sql.window import Window

taxi_df = spark.table("samples.nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #1  
# MAGIC Calcular la duración de cada viaje en minutos (diferencia entre `tpep_dropoff_datetime` y `tpep_pickup_datetime`). Luego agrupar por `pickup_zip` y obtener: duración promedio, número total de viajes. Ordenar de mayor a menor duración promedio.

# COMMAND ----------

# Filtrar viajes con timestamps válidos
valid_trips = taxi_df.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull()
)

# Añadir columna de duración en minutos
trips_with_duration = valid_trips.withColumn(
    "trip_duration_minutes",
    (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))/60
)

# Agrupar por pickup_zip
result1 = trips_with_duration.groupBy("pickup_zip").agg(
    _avg("trip_duration_minutes").alias("avg_duration_minutes"),
    _count("*").alias("num_trips")
).orderBy(desc("avg_duration_minutes"))

result1.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #2  
# MAGIC Usar `trip_distance`: identificar los viajes con distancia mayor que 10 km. Calcular qué porcentaje representan del total, y comparar la tarifa promedio entre esos viajes y los que tienen distancia ≤ 10 km.

# COMMAND ----------

total_trips = taxi_df.count()

trips_gt10km = taxi_df.filter(col("trip_distance") > 10)
num_gt10km = trips_gt10km.count()
percentage_gt10km = num_gt10km / total_trips * 100
print(f"Porcentaje de viajes >10 km: {percentage_gt10km:.2f}%")

avg_fare_gt10km = trips_gt10km.agg(_avg("fare_amount")).collect()[0][0]
trips_le10km = taxi_df.filter(col("trip_distance") <= 10)
avg_fare_le10km = trips_le10km.agg(_avg("fare_amount")).collect()[0][0]

print(f"Tarifa promedio (>10 km): {avg_fare_gt10km:.2f}")
print(f"Tarifa promedio (≤10 km): {avg_fare_le10km:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #3  
# MAGIC Determinar las **top 10** valores de `pickup_zip` con mayor número de viajes en el año 2016. Mostrar `pickup_zip` + número de viajes en orden descendente.

# COMMAND ----------

trips2019 = taxi_df.filter(year(col("tpep_pickup_datetime")) == 2016)
top10_zips = trips2019.groupBy("pickup_zip").count().orderBy(desc("count")).limit(10)
top10_zips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #4  
# MAGIC Analizar la relación entre `trip_distance` y `fare_amount`: agrupar por intervalos de distancia (por ejemplo cada 5 km) y para cada intervalo calcular: tarifa promedio, distancia promedio, número de viajes.

# COMMAND ----------

taxi_dist_fare = taxi_df.filter(col("trip_distance").isNotNull() & col("fare_amount").isNotNull()) \
    .withColumn("distance_bucket", (floor(col("trip_distance")/5)*5).cast("int")) \
    .groupBy("distance_bucket").agg(
        _avg("fare_amount").alias("avg_fare"),
        _avg("trip_distance").alias("avg_distance"),
        _count("*").alias("num_trips")
    ).orderBy("distance_bucket")

taxi_dist_fare.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #5  
# MAGIC Extraer la hora del día desde `tpep_pickup_datetime`. Luego agrupar por esa hora para calcular: número de viajes por hora, duración promedio por hora. Ordenar por número de viajes descendente.

# COMMAND ----------

hourly_analysis = taxi_df.filter(col("tpep_pickup_datetime").isNotNull()) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("trip_duration_minutes", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))/60) \
    .groupBy("pickup_hour").agg(
        _count("*").alias("num_trips"),
        _avg("trip_duration_minutes").alias("avg_duration_minutes")
    ).orderBy(desc("num_trips"))

hourly_analysis.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #6  
# MAGIC Crear un análisis por rangos de tarifa (`fare_amount`): agrupar los viajes por buckets de tarifa cada 10 USD, y para cada bucket calcular número de viajes, distancia promedio y duración promedio.

# COMMAND ----------

valid_fare_trips = taxi_df.filter(col("fare_amount").isNotNull())

trips_fare_bucket = valid_fare_trips.withColumn(
    "fare_bucket",
    (floor(col("fare_amount")/10)*10).cast("int")
)

result6 = trips_fare_bucket.groupBy("fare_bucket").agg(
    _count("*").alias("num_trips"),
    _avg("trip_distance").alias("avg_distance"),
    _avg((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))/60).alias("avg_trip_duration_minutes")
).orderBy("fare_bucket")

result6.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #7  
# MAGIC Crear un ranking de rutas “origen-zip → destino-zip”: combinar `pickup_zip` y `dropoff_zip` para formar una “ruta”, agrupar por esa ruta, contar número de viajes y tarifa promedio; mostrar las top 5 rutas más frecuentes.

# COMMAND ----------

route_ranking = taxi_df.filter(col("pickup_zip").isNotNull() & col("dropoff_zip").isNotNull()) \
    .withColumn("ruta", concat_ws("→", col("pickup_zip").cast("string"), col("dropoff_zip").cast("string"))) \
    .groupBy("ruta").agg(
        _count("*").alias("num_trips"),
        _avg("fare_amount").alias("avg_fare")
    ).orderBy(desc("num_trips")).limit(5)

route_ranking.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio #8  
# MAGIC (Extra) Crear un análisis comparativo anual: extraer el año de `tpep_pickup_datetime`, y para cada año calcular: número de viajes, tarifa promedio, distancia promedio. Ordenar por año ascendente.

# COMMAND ----------

yearly_analysis = taxi_df.withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_year").agg(
        _count("*").alias("num_trips"),
        _avg("fare_amount").alias("avg_fare"),
        _avg("trip_distance").alias("avg_distance")
    ).orderBy("pickup_year")

yearly_analysis.show()