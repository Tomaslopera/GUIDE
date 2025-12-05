# Databricks notebook source
# MAGIC %md
# MAGIC # Introducción a Databricks
# MAGIC
# MAGIC ## 1. ¿Qué es Databricks?
# MAGIC
# MAGIC Databricks es una plataforma unificada de análisis de datos, ciencia de datos e inteligencia artificial que opera sobre infraestructura en la nube. 
# MAGIC
# MAGIC Fue creada por los fundadores originales de Apache Spark y está diseñada específicamente para trabajar con grandes volúmenes de datos, tanto estructurados como no estructurados.
# MAGIC
# MAGIC **¿Por qué Databricks?** La plataforma elimina la complejidad de configurar y mantener infraestructura distribuida, permitiendo que equipos de datos se enfoquen en extraer valor de los datos en lugar de administrar servidores.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Componentes principales
# MAGIC
# MAGIC Para ingenieros de datos:
# MAGIC
# MAGIC ### **Workspace y Notebooks**
# MAGIC El entorno colaborativo donde se crea, edita y ejecuta código, visualizaciones y documentación. Los notebooks funcionan como cuadernos interactivos que combinan código ejecutable, resultados y documentación en un solo lugar.
# MAGIC
# MAGIC ### **Clústeres y Computación**
# MAGIC El motor que ejecuta el procesamiento distribuido utilizando Apache Spark. Los clústeres manejan el escalado automático y la gestión de recursos, permitiendo procesar desde gigabytes hasta petabytes de datos.
# MAGIC
# MAGIC ### **Almacenamiento y Formatos Optimizados**
# MAGIC Integración nativa con Data Lakes y Data Warehouses. Databricks utiliza formatos optimizados como Delta Lake que proporcionan transacciones ACID, versionamiento de datos y mejor rendimiento.
# MAGIC
# MAGIC ### **Gobernanza y Seguridad**
# MAGIC Sistema integral para gestión de usuarios, permisos, acceso a datos, catálogos de metadatos (Unity Catalog) y políticas de gobierno de datos. Esto garantiza que los datos correctos estén disponibles para las personas correctas.
# MAGIC
# MAGIC ### **Integración Multidisciplinaria**
# MAGIC La plataforma permite construir pipelines de datos, realizar transformaciones, análisis exploratorio, desarrollar modelos de machine learning y colaborar entre diferentes disciplinas (ingenieros de datos, analistas, científicos de datos).
# MAGIC
# MAGIC ### Organización en capas
# MAGIC
# MAGIC Estos componentes se pueden agrupar en tres capas fundamentales:
# MAGIC
# MAGIC 1. **Capa de Infraestructura y Computación**: Clústeres, recursos de cómputo, networking
# MAGIC 2. **Capa de Datos y Gobernanza**: Almacenamiento, catálogos, seguridad, control de acceso
# MAGIC 3. **Capa de Procesamiento y Análisis**: Spark, SQL, ML, notebooks, visualizaciones
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Arquitectura de alto nivel
# MAGIC
# MAGIC La arquitectura de Databricks se organiza en dos planos principales que trabajan en conjunto:
# MAGIC
# MAGIC ### **Control Plane (Plano de Control)**
# MAGIC Gestionado completamente por Databricks, incluye:
# MAGIC - Interfaz de usuario web
# MAGIC - APIs REST para automatización
# MAGIC - Gestión de cuentas y workspaces
# MAGIC - Orquestación de trabajos
# MAGIC - Catálogo de metadatos
# MAGIC
# MAGIC ### **Compute Plane (Plano de Computación)**
# MAGIC Donde ocurre el procesamiento real de datos:
# MAGIC - Clústeres Apache Spark en ejecución
# MAGIC - Lectura y escritura de datos desde/hacia almacenamiento
# MAGIC - Ejecución de trabajos y notebooks
# MAGIC - Procesamiento distribuido de datos
# MAGIC
# MAGIC ### Flujo de trabajo típico
# MAGIC
# MAGIC ```
# MAGIC Usuario → Notebook en Workspace → Clúster Spark → 
# MAGIC Lectura de datos desde almacenamiento → 
# MAGIC Procesamiento distribuido → 
# MAGIC Resultados (tablas/vistas/dashboards)
# MAGIC ```
# MAGIC
# MAGIC **¿Por qué importa esta arquitectura?**
# MAGIC - **Escalabilidad**: Permite procesar desde kilobytes hasta petabytes
# MAGIC - **Tolerancia a fallos**: Si un nodo falla, el trabajo continúa
# MAGIC - **Separación de responsabilidades**: La infraestructura es manejada por Databricks
# MAGIC - **Eficiencia de recursos**: Se paga solo por lo que se usa, con escalado automático
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Primeros pasos en la plataforma
# MAGIC
# MAGIC ### Instrucciones para comenzar
# MAGIC
# MAGIC 1. **Acceder al workspace**: Se debe abrir el workspace en Databricks (o utilizar el proporcionado por el instructor)
# MAGIC
# MAGIC 2. **Crear un notebook**: 
# MAGIC    - Se hace clic en "Create" → "Notebook"
# MAGIC    - Se asigna un nombre descriptivo
# MAGIC    - Se selecciona el lenguaje (Python, SQL, Scala o R)
# MAGIC
# MAGIC 3. **Configurar un clúster**:
# MAGIC    - Se adjunta o crea un clúster nuevo
# MAGIC    - Recomendación: Se puede usar un clúster tipo **All-Purpose** para exploración interactiva
# MAGIC    - Para producción, se recomienda considerar clústeres tipo **Job** (más económicos)
# MAGIC    - Para el caso de Free Edition, se tiene por defecto Serverless.
# MAGIC
# MAGIC 4. **Navegar la interfaz principal**:
# MAGIC    - **Workspace/Repos**: Se organiza el código y proyectos
# MAGIC    - **Data**: Se exploran catálogos, esquemas y tablas disponibles
# MAGIC    - **Compute**: Se administran y monitorean clústeres
# MAGIC    - **SQL**: Editor de consultas y dashboards
# MAGIC
# MAGIC 5. **Verificar el estado**: Se confirma que el clúster está en estado **"Running"** antes de ejecutar código
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. SparkContext
# MAGIC
# MAGIC El SparkContext es el punto de entrada principal a la funcionalidad de Apache Spark. Representa la conexión con el clúster de Spark y actúa como el coordinador de todas las operaciones distribuidas. En Databricks, el SparkContext se crea automáticamente y está disponible a través de la variable spark (SparkSession) y sc (SparkContext).
# MAGIC Conceptos clave:
# MAGIC
# MAGIC SparkSession (spark): API de alto nivel introducida en Spark 2.0+ que unifica SparkContext, SQLContext y HiveContext. Es el punto de entrada recomendado para trabajar con DataFrames y SQL.
# MAGIC SparkContext (sc): API de bajo nivel que maneja la conexión con el clúster, configuración y creación de RDDs (Resilient Distributed Datasets).
# MAGIC
# MAGIC En la mayoría de casos, se trabajará con spark (SparkSession), pero es importante entender que internamente utiliza el SparkContext.
# MAGIC Relación entre SparkSession y SparkContext

# COMMAND ----------

print(f"SparkSession disponible: {spark}")
print(f"SparkContext disponible: {sc}")

# COMMAND ----------

numeros = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

print(f"Tipo de objeto: {type(numeros)}")
print(f"Número de particiones: {numeros.getNumPartitions()}")
print(f"Primeros 5 elementos: {numeros.take(5)}")

# COMMAND ----------

