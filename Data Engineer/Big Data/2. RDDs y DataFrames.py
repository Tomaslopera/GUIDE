# Databricks notebook source
# MAGIC %md
# MAGIC # RDDs y DataFrames
# MAGIC ### Edición para Databricks Free / Serverless  
# MAGIC  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 1. Introducción 
# MAGIC Como ya se dijo, Apache Spark es un motor de procesamiento distribuido que opera sobre clústeres para permitir análisis a gran escala. En sus inicios, su núcleo era la abstracción de los RDDs (Resilient Distributed Datasets). Con el tiempo, para facilitar la expresividad, la optimización automática y la compatibilidad con APIs tabulares/SQL, se introdujeron DataFrames y Datasets.  
# MAGIC  
# MAGIC Este notebook explica esa transición, cómo funcionan internamente estos modelos de datos, cuándo es preferible usar uno u otro, y las particularidades del entorno “Free/Serverless” de Databricks.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 2. ¿Qué son los RDDs (Resilient Distributed Datasets)?  
# MAGIC Los RDDs son la abstracción fundacional de Spark: una colección **inmutable**, **distribuida** y **tolerante a fallos** que puede residir en memoria o en disco y permite aplicar transformaciones paralelas sobre un clúster.  
# MAGIC  
# MAGIC **Características clave**  
# MAGIC - *Resilient*: Spark mantiene el linaje (registro de transformaciones) para poder reconstruir particiones ante fallas.  
# MAGIC - *Distributed*: Los datos se particionan y se reparten entre nodos del clúster.  
# MAGIC - *Dataset*: Puede contener objetos arbitrarios (Python, Scala, Java) sin esquema rígido.  
# MAGIC  
# MAGIC **Limitaciones de los RDDs**  
# MAGIC - No disponen de esquema estructurado (no se define por defecto “columnas” con tipos).  
# MAGIC - No permiten optimización automática sobre un plan de consulta tabular (como proyección de columnas, push-down, etc.).  
# MAGIC - Su API es de bajo nivel, más verbosa para transformaciones analíticas comunes.  
# MAGIC - En muchos entornos gestionados (como la edición gratuita de Databricks), el acceso directo a `SparkContext` (*sc.parallelize*, etc.) puede no estar habilitado o no es el camino recomendado.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 3. Evolución hacia los DataFrames  
# MAGIC Para abordar esas limitaciones, Spark introdujo la abstracción de **DataFrames** (y en Scala/Java, los **Datasets**). Un DataFrame es una colección distribuida de datos estructurados en **columnas con nombre y tipo**, similar a una tabla relacional.  
# MAGIC  
# MAGIC Las ventajas fundamentales que introduce:  
# MAGIC - Permite que Spark genere un **plan lógico** que luego se optimice internamente (mediante el optimizador **Catalyst**) y se ejecute con un motor eficiente (**Tungsten**).  
# MAGIC - Integra soporte para SQL y funciones analíticas, lo que facilita la interoperabilidad entre Spark, SQL, Python, Scala.  
# MAGIC - Mejora la legibilidad del código y reduce la necesidad de manejar el linaje manualmente o gestionar particiones a bajo nivel.  
# MAGIC  
# MAGIC En resumen: los DataFrames permiten que el usuario exprese “qué” quiere hacer con los datos (consulta tabular), y Spark decide “cómo” hacerlo eficientemente.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 4. ¿Cuándo usar RDDs vs DataFrames?  
# MAGIC **Tabla de comparación simplificada**  
# MAGIC | Situación | RDDs | DataFrames |
# MAGIC |-----------|------|-----------|
# MAGIC | Datos sin estructura / objetos arbitrarios | ✅ | |
# MAGIC | Necesidad de control fino de cada partición y línea de ejecución | ✅ | |
# MAGIC | Datos con estructura tabular (columnas/tipos) | | ✅ |
# MAGIC | Consultas SQL, análisis, agregaciones comunes | | ✅ |
# MAGIC | Querer optimización automática sin escribir mucho código de bajo nivel | | ✅ |
# MAGIC | Algoritmos distribuidos personalizados o código legacy | ✅ | |
# MAGIC  
# MAGIC En la práctica, **los DataFrames son la opción recomendada**. Los RDDs se reservan para casos muy específicos.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 5. Particularidad de la edición gratuita de Databricks y los RDDs  
# MAGIC En la edición gratuita de Databricks (“Free/Serverless”), el entorno está optimizado para DataFrames y SQL. En muchos casos, no se da acceso disponible o recomendado al `SparkContext` (`sc.parallelize`, etc.). Por esta razón, este notebook **no incluye ejemplos de ejecución con RDDs**, aunque se explican a nivel teórico. La práctica se centra exclusivamente en DataFrames para asegurar compatibilidad con el entorno gestionado.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 6. Ejecución perezosa (Lazy Execution) y el plan de ejecución de Spark  
# MAGIC Spark emplea un modelo de **evaluación diferida** (“lazy execution”): cuando se invocan transformaciones (por ejemplo `filter`, `select`, `groupBy`), no ocurre ejecución inmediata. En su lugar, Spark construye un **gráfico acíclico dirigido (DAG – Directed Acyclic Graph)** que representa todas las operaciones que se requieren. La ejecución sólo sucede cuando una acción “desencadena” el proceso, como `show()` o `count()`.  
# MAGIC  
# MAGIC **Flujo interno simplificado**  
# MAGIC 1. Usuario define transformaciones → Spark crea plan lógico.  
# MAGIC 2. Spark usa **Catalyst** para optimizar el plan lógico: combina operaciones, reordena filtros, elimina columnas innecesarias.  
# MAGIC 3. Se genera un **plan físico** optimizado, que puede implicar *shuffles*, *particionamiento*, etc.  
# MAGIC 4. El motor **Tungsten** ejecuta el plan físico aprovechando buffers binarios, óptima gestión de memoria y CPU.  
# MAGIC  
# MAGIC Esta arquitectura permite que el usuario trabaje a nivel alto (DataFrames/SQL) y Spark se encargue de la eficiencia de ejecución.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 7. Transformaciones y Acciones en Spark  
# MAGIC ### 7.1 Transformaciones  
# MAGIC Las transformaciones crean un nuevo DataFrame (o RDD) a partir de otro. No desencadenan ejecución inmediata.  
# MAGIC Ejemplos en DataFrames:  
# MAGIC - `select("columna")` → proyección de columnas  
# MAGIC - `filter(col("edad") > 30)` → filtrado de filas  
# MAGIC - `groupBy("ciudad")` → preparación para agregaciones  
# MAGIC - `withColumn("nueva", expr(...))` → creación de columnas derivadas  
# MAGIC  
# MAGIC Durante estas operaciones, Spark registra internamente el DAG. Sólo se registra lo que se hará, no se ejecuta.  
# MAGIC  
# MAGIC ### 7.2 Acciones  
# MAGIC Las acciones desencadenan la computación real: Spark evalúa el DAG, lo optimiza y lo ejecuta sobre el clúster.  
# MAGIC Ejemplos comunes:  
# MAGIC - `show()` → muestra un número limitado de filas  
# MAGIC - `count()` → cuenta el número de filas  
# MAGIC - `collect()` → trae los datos al driver (útil solo para conjuntos pequeños)  
# MAGIC - `take(n)` → toma las primeras *n* filas  
# MAGIC - `write` → escribe un DataFrame en disco o en la nube  
# MAGIC  
# MAGIC ### 7.3 Transformaciones *narrow* vs *wide*  
# MAGIC - **Narrow (estrechas)**: cada partición produce datos para exactamente una partición descendiente; por ejemplo `map()`, `filter()` en RDDs, `select()` en DataFrames. No requieren *shuffle*.  
# MAGIC - **Wide (anchas)**: requieren redistribución de datos entre particiones (shuffle); por ejemplo `groupBy()`, `join()`, `distinct()`. Tienden a ser más costosas en tiempo y recursos.  
# MAGIC  
# MAGIC ### 7.4 Ventajas del modelo perezoso  
# MAGIC - Spark puede **fusionar** múltiples transformaciones antes de ejecutar, reduciendo el número de pasadas por los datos.  
# MAGIC - Permite **optimización automática** (Catalyst) que mejora la eficiencia sin que el usuario intervenga en la planificación del clúster.  
# MAGIC - Reduce operaciones innecesarias y optimiza el uso de memoria y CPU.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 8. Conceptos clave del motor interno  
# MAGIC - **Catalyst Optimizer**: el motor de análisis de Spark que genera, transforma y optimiza el plan lógico antes de su ejecución física.  
# MAGIC - **Tungsten Execution Engine**: optimiza la ejecución física, acceso de bajo nivel a los datos, administración eficiente de memoria/CPU.  
# MAGIC - **Plan lógico vs plan físico**: el usuario escribe transformaciones (plan lógico), Spark las convierte en un plan físico optimizado que se ejecuta. Se puede inspeccionar con `df.explain(mode="formatted")`.  
# MAGIC  
# MAGIC ---
# MAGIC  
# MAGIC ## 9. En resumen  
# MAGIC - Los RDDs fueron la capa de bajo nivel de Spark: muy flexibles, pero más verbosos y sin optimización automática.  
# MAGIC - Los DataFrames representan la evolución natural: combinan expresividad, esquema, soporte SQL y optimización automática.  
# MAGIC - Las transformaciones y acciones permiten que Spark aplique su modelo de ejecución diferida, optimización interna y paralelismo de forma eficiente.  
# MAGIC - En entornos como la edición gratuita de Databricks, se recomienda centrar la enseñanza y práctica en DataFrames y SQL.  
# MAGIC  
# MAGIC ---  
# MAGIC  
# MAGIC  

# COMMAND ----------

print(f"SparkSession: {spark}")  
print(f"Versión de Spark: {spark.version}")  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 1. Creación de DataFrames  
# MAGIC
# MAGIC  

# COMMAND ----------

from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  

personas_df = spark.createDataFrame(  
    [  
        ("Ana", 28, "Ingeniería"),  
        ("Carlos", 35, "Marketing"),  
        ("Beatriz", 42, "Finanzas"),  
        ("David", 31, "Ingeniería"),  
        ("Elena", 29, "Marketing"),  
    ],  
    ["nombre", "edad", "departamento"]  
)  

personas_df.show()  
personas_df.printSchema()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 2. Otra creación con esquema explícito  
# MAGIC  

# COMMAND ----------

schema = StructType([  
    StructField("nombre", StringType(), False),  
    StructField("edad", IntegerType(), True),  
    StructField("departamento", StringType(), True)  
])  

personas_con_schema_df = spark.createDataFrame(  
    [  
        Row(nombre="Ana", edad=28, departamento="Ingeniería"),  
        Row(nombre="Carlos", edad=35, departamento="Marketing"),  
        Row(nombre="Beatriz", edad=42, departamento="Finanzas"),  
    ],  
    schema=schema  
)  

personas_con_schema_df.show(truncate=False)  
personas_con_schema_df.printSchema()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 3. Transformaciones y Acciones básicas (DataFrames)  
# MAGIC  

# COMMAND ----------

from pyspark.sql.functions import col, lower, lit  

df = personas_df  

df_trans = (  
    df.select("nombre", "edad", "departamento")  
      .withColumn("es_mayor_30", col("edad") > 30)  
      .withColumn("departamento_lower", lower(col("departamento")))  
      .filter(col("edad") >= 28)  
)  

df_trans.show()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 4. Selección, filtrado y ordenamiento  
# MAGIC  

# COMMAND ----------

df.select("nombre", "edad").show()  
df.select(col("nombre"), (col("edad") + lit(1)).alias("edad_mas_1")).show()  
df.filter(col("departamento") == "Ingeniería").orderBy(col("edad").desc()).show()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 5. Agrupaciones y agregaciones  
# MAGIC  

# COMMAND ----------

from pyspark.sql.functions import avg, sum as _sum, count, countDistinct  

ventas_df = spark.createDataFrame(  
    [  
        ("Enero", 1000, "Laptop"),  
        ("Febrero", 1500, "Mouse"),  
        ("Enero", 800, "Mouse"),  
        ("Marzo", 2000, "Monitor"),  
        ("Febrero", 1200, "Teclado"),  
        ("Marzo", 1800, "Laptop"),  
        ("Enero", 500, "Teclado"),  
    ],  
    ["mes", "monto", "producto"]  
)  

ventas_df.groupBy("mes").agg(  
    _sum("monto").alias("total_mes"),  
    count("*").alias("n_transacciones"),  
    countDistinct("producto").alias("productos_unicos")  
).orderBy("mes").show()  

ventas_df.groupBy("producto").agg(avg("monto").alias("avg_monto")).orderBy(col("avg_monto").desc()).show()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 6. Joins entre DataFrames  
# MAGIC  

# COMMAND ----------

empleados_df = spark.createDataFrame(  
    [  
        (1, "Ana", "ING"),  
        (2, "Carlos", "MKT"),  
        (3, "Beatriz", "FIN"),  
        (4, "David", "ING"),  
    ],  
    ["id", "nombre", "cod_dep"]  
)  

departamentos_df = spark.createDataFrame(  
    [  
        ("ING", "Ingeniería"),  
        ("MKT", "Marketing"),  
        ("FIN", "Finanzas"),  
    ],  
    ["cod_dep", "departamento"]  
)  

empleados_join = (  
    empleados_df.join(departamentos_df, on="cod_dep", how="left")  
                 .select("id", "nombre", "departamento")  
)  
empleados_join.show()  

# COMMAND ----------

# MAGIC %md  
# MAGIC ### 7. Uso de SQL con DataFrames  
# MAGIC Se registra una vista temporal y se ejecuta una consulta SQL
# MAGIC  

# COMMAND ----------

df.createOrReplaceTempView("personas")  

spark.sql("""  
SELECT departamento, AVG(edad) AS edad_promedio, COUNT(*) AS n_personas  
FROM personas  
GROUP BY departamento  
ORDER BY edad_promedio DESC  
""").show()  