# Databricks notebook source
# MAGIC %md
# MAGIC #### Modelado dimensional de Resultados Electorales
# MAGIC
# MAGIC Este notebook implementa el modelado de la capa Gold del pipeline de resultados electorales. A partir de la tabla consolidada de la capa Silver, construye un modelo analítico basado en un esquema copo de nieve (*snowflake schema*), generando una tabla de hechos, sus dimensiones asociadas y una tabla resumen orientada al análisis por mesa.
# MAGIC
# MAGIC Como resultado, se generan las tablas analíticas correspondientes al proceso electoral 2025, que constituyen la capa Gold y sirven como base para consultas, visualizaciones y análisis multidimensionales.
# MAGIC
# MAGIC ## Principales tareas realizadas
# MAGIC
# MAGIC - Construcción de la tabla de hechos, registrando una fila por campo de escrutinio para cada categoría y mesa.
# MAGIC - Construcción de las dimensiones geográficas (Distrito, Sección, Circuito y Mesa) y de las dimensiones de Categoría y Campo.
# MAGIC - Normalización de la jerarquía geográfica mediante un esquema copo de nieve.
# MAGIC - Construcción de una tabla resumen de votos agregados por mesa mediante la pivotación de los resultados electorales.
# MAGIC
# MAGIC ## Decisiones técnicas
# MAGIC
# MAGIC La capa Gold se implementa mediante un esquema copo de nieve (*snowflake schema*), donde las dimensiones geográficas se encuentran normalizadas siguiendo la organización administrativa del sistema electoral:
# MAGIC
# MAGIC ```
# MAGIC Distrito
# MAGIC    ↓
# MAGIC Sección
# MAGIC    ↓
# MAGIC Circuito
# MAGIC    ↓
# MAGIC Mesa
# MAGIC ```
# MAGIC
# MAGIC Esta decisión responde a las características propias del dominio electoral y presenta las siguientes ventajas:
# MAGIC
# MAGIC - Representa fielmente la jerarquía territorial, respetando las relaciones naturales entre distrito, sección, circuito y mesa.
# MAGIC - Reduce la redundancia de información, evitando repetir atributos geográficos en cada mesa o en la tabla de hechos.
# MAGIC - Garantiza la consistencia del modelo mediante claves únicas jerárquicas y relaciones explícitas entre dimensiones.
# MAGIC - Facilita la integración futura de otros conjuntos de datos electorales que compartan la misma estructura territorial.
# MAGIC - Favorece la mantenibilidad del modelo, concentrando cada entidad geográfica en una única dimensión.
# MAGIC
# MAGIC La tabla de hechos representa la unidad mínima de observación del escrutinio: **una fila corresponde a la cantidad registrada para un campo de escrutinio, una categoría y una mesa**.
# MAGIC
# MAGIC La dimensión `g2025_dim_campo` modela de forma homogénea tanto las agrupaciones participantes como los distintos totales y métricas del escrutinio (votos en blanco, votos nulos, votos recurridos, votos impugnados, votos totales e inscriptos), permitiendo representar todas las observaciones mediante una única estructura dimensional.
# MAGIC
# MAGIC ## Outputs principales
# MAGIC
# MAGIC | Tabla | Descripción |
# MAGIC |-------|-------------|
# MAGIC | `g2025_fact_resultados` | Tabla de hechos con las cantidades registradas por mesa, categoría y campo. |
# MAGIC | `g2025_dim_distrito` | Dimensión de distritos electorales. |
# MAGIC | `g2025_dim_seccion` | Dimensión de secciones electorales. |
# MAGIC | `g2025_dim_circuito` | Dimensión de circuitos electorales. |
# MAGIC | `g2025_dim_mesa` | Dimensión de mesas electorales. |
# MAGIC | `g2025_dim_categoria` | Dimensión de categorías electorales. |
# MAGIC | `g2025_dim_campo` | Dimensión de campos de escrutinio. |
# MAGIC | `g2025_resumen_mesa` | Tabla resumen con los resultados consolidados por mesa. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Configuración del entorno

# COMMAND ----------

# Importar librerías y parámetros
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, concat_ws
from pyspark import StorageLevel

# COMMAND ----------

# MAGIC %run Workspace/resultados-electorales-pipeline/00_parametros

# COMMAND ----------

# Funciones auxiliares para la construcción de claves unicas
def build_id_circuito():
    return F.concat_ws("", "distrito", "seccion", "circuito")

def build_id_seccion():
    return F.concat_ws("", "distrito", "seccion")

def build_id_campo():
    return F.concat_ws(
        "-",
        "distrito",
        "categoria",
        "campo_numero",
        "campo_descripcion"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Carga de datos desde Silver

# COMMAND ----------

# Cargar la tabla Silver consolidada
df_silver = spark.table(tabla_silver_consolidada)

# COMMAND ----------

# MAGIC %md
# MAGIC Construcción tabla de hechos

# COMMAND ----------

# Construir y persistir la tabla de hechos
(
    df_silver
    .select(
        "id_mesa",
        build_id_campo().alias("id_campo"),
        "cantidad"
    )
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_fact_resultados)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Construcción tablas de dimensiones

# COMMAND ----------

# Construir y persistir la dimensión Mesa
(
    df_silver
    .select(
        "id_mesa",
        build_id_circuito().alias("id_circuito"),
        "mesa",
        "tipo_mesa"
    ).dropDuplicates(['id_mesa', 'id_circuito', 'mesa', 'tipo_mesa'])
    .distinct()
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_mesa)
)

# COMMAND ----------

# Construir y persistir la dimensión Circuito preservando la jerarquía geográfica
# Se filtran los circuitos con valores no nulos
(
    df_silver
    .filter(F.col("circuito").isNotNull())
    .select(
        build_id_circuito().alias("id_circuito"),
        build_id_seccion().alias("id_seccion"),
        "circuito",
        "circuito_descripcion"
    )
    .distinct()
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_circuito)
)

# COMMAND ----------

# Construir y persistir la dimensión Sección preservando la jerarquía geográfica
# Se filtran las secciones con valores no nulas
(
    df_silver
    .filter(F.col("seccion").isNotNull())
    .select(
        build_id_seccion().alias("id_seccion"),
        F.col("distrito").alias("id_distrito"),
        F.col("seccion").alias("seccion_numero"),
        "seccion_descripcion"
    )
    .distinct()
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_seccion)
)

# COMMAND ----------

# Construir y persistir la dimensión Distrito
mapping_distritos = F.create_map(
    *[F.lit(x) for kv in iddistritos.items() for x in kv]
)

(
    df_silver
    .select(
        F.col("distrito").alias("id_distrito")
    )
    .distinct()
    .withColumn(
        "distrito",
        mapping_distritos[F.col("id_distrito")]
    )
    .select("id_distrito", "distrito")
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_distrito)
)

# COMMAND ----------

# Construir y persistir la dimensión Categoría
(
    df_silver
    .select(
        F.col("categoria").alias("id_categoria"),
        F.col("categoria").alias("codigo")
    )
    .distinct()
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_categoria)
)

# COMMAND ----------

# Construir la dimensión Campo mediante una clave compuesta (id_campo)
(
    df_silver
    .select(
        F.concat_ws(
            "-",
            "distrito",
            "categoria",
            "campo_numero",
            "campo_descripcion"
        ).alias("id_campo"),
        F.col("distrito").alias("id_distrito"),
        F.col("categoria").alias("id_categoria"),
        F.col("campo_numero").alias("campo_numero"),
        F.col("campo_descripcion").alias("campo_descripcion")
    )
    .distinct()
    .write
    .mode("overwrite")
    .insertInto(tabla_gold_dim_campo)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Resumen de resultados por mesa

# COMMAND ----------


# Pivotear los resultados para obtener una fila por mesa y categoria para las categorias agregadas (totales, nulos, blancos, recurridos, impugnados)
mesas = (
    df_silver
    .filter(col("registro")=="MESA")
    .groupBy(
        "id_mesa",
        "distrito",
        "seccion",
        "seccion_descripcion",
        "circuito",
        "circuito_descripcion",
        "mesa",
        "categoria",
        "tipo_mesa"
    )
    .pivot("campo_numero")
    .agg(F.first("cantidad"))
)

# COMMAND ----------

# Calcular los votos afirmativos y la cantidad de listas por mesa
afirmativos = (
    df_silver
    .filter(F.col("registro") == "AGRUPACION")
    .groupBy("categoria", "id_mesa")
    .agg(
        F.sum("cantidad").alias("votos_afirmativos"),
        F.count("*").alias("listas")
    )
)

# COMMAND ----------

# Integrar los votos afirmativos al resumen por mesa
df_mesas = (
    mesas
    .join(
        afirmativos,
        on=["categoria", "id_mesa"],
        how="left"
    )
)

# COMMAND ----------

# Estandarizar los nombres de las columnas de salida
for nombre_columna, codigo in codigos_mesa.items():
    df_mesas = df_mesas.withColumnRenamed(codigo, nombre_columna)

# COMMAND ----------

# Ordenar y persistir la tabla de resumen por mesa
(df_mesas
.select(*spark.table(tabla_gold_resumen_mesa).columns)
.write
.mode("overwrite")
.insertInto(tabla_gold_resumen_mesa)
)
