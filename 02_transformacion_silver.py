# Databricks notebook source
# MAGIC %md
# MAGIC #### Consolidación y normalización de Resultados Electorales
# MAGIC
# MAGIC Este notebook implementa el proceso de transformación de la capa Silver del pipeline de resultados electorales. A partir de la tabla consolidada de la capa Bronze, aplica reglas de estandarización y limpieza, incorpora atributos derivados y realiza validaciones de calidad para garantizar la consistencia de los datos.
# MAGIC
# MAGIC Como resultado, se genera una tabla normalizada que constituye la base para la construcción de la capa Gold.
# MAGIC
# MAGIC **Principales tareas realizadas**
# MAGIC
# MAGIC - Lectura de la tabla consolidada de la capa Bronze y selección de las columnas relevantes para el modelo Silver.
# MAGIC - Estandarización del esquema mediante la selección, renombrado y ordenamiento de columnas de acuerdo con el modelo de datos definido para la capa Silver.
# MAGIC - Normalización de códigos identificatorios y geográficos mediante padding para preservar su formato y longitud.
# MAGIC - Construcción del identificador único de mesa (`id_mesa`) a partir de los códigos geográficos y electorales.
# MAGIC - Identificación y clasificación de los distintos tipos de mesa, incluyendo mesas comunes, de electores residentes en el exterior y de personas privadas de la libertad.
# MAGIC - Validación de la integridad de los datos transformados, verificando la completitud de los identificadores geográficos en las mesas normales, la consistencia de las mesas compensatorias, la normalización de las etiquetas de categorías, la ausencia de valores nulos en los votos y la longitud esperada del identificador de mesa.
# MAGIC - Persistencia de la tabla de resultados electorales normalizada.
# MAGIC
# MAGIC
# MAGIC **Outputs principales**
# MAGIC
# MAGIC  Tabla consolidada de resultados electorales normalizados.
# MAGIC  
# MAGIC `g2025_resultados_consolidado` 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Configuración del entorno

# COMMAND ----------

# Importar librerías y parámetros
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run Workspace/resultados-electorales-pipeline/00_parametros

# COMMAND ----------

# MAGIC %md
# MAGIC Parámetros de transformación

# COMMAND ----------

# Definir columnas bronze que se utilizan en silver
columnas_bronze = [ "dist_codigo", "secc_codigo", "secc_descrip", "circ_codigo", "circ_descrip", "zona_codigo",
    "carg_codigo", "ppar_codigo", "ppar_descrip", "cant", "totins"]

# COMMAND ----------

# Definir columnas finales de consolidación
columnas_silver = [ "id_mesa", "distrito", "seccion", "seccion_descripcion", "circuito", "circuito_descripcion",
    "mesa", "categoria", "campo", "campo_numero", "campo_descripcion", "cantidad",  "registro", "tipo_mesa"]

# COMMAND ----------

# Equivalencia de nombres Bronze-Silver
renombre_columnas = {"dist_codigo": "distrito", "secc_codigo": "seccion", "secc_descrip": "seccion_descripcion",
    "circ_codigo": "circuito", "circ_descrip": "circuito_descripcion", "zona_codigo": "mesa", "carg_codigo": "categoria", "ppar_codigo": "campo_numero", "ppar_descrip": "campo_descripcion", "cant": "cantidad",
    "totins": "inscriptos"}

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura y consolidación Bronze

# COMMAND ----------

# Leer la tabla Bronze consolidada
df_silver = (
    spark.table(f"{tabla_bronze_resultados}")
    .select(*columnas_bronze)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Estandarización de columnas

# COMMAND ----------

#Renombar columnas
for origen, destino in renombre_columnas.items():
    df_silver = df_silver.withColumnRenamed(origen, destino)

# COMMAND ----------

# MAGIC %md
# MAGIC Normalización de identificadores

# COMMAND ----------

# Las columnas de otros tipos se mantienen sin cambios
# Esto evita conservar valores vacíos como texto y unifica el tratamiento de datos faltantes.

df_silver = df_silver.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) if isinstance(df_silver.schema[c].dataType, StringType)
    else col(c)
    for c in df_silver.columns
])

# COMMAND ----------

# En caso de que un valor tenga más caracteres que la longitud esperada, se conservan
# únicamente los últimos dígitos antes de aplicar el padding.

# Normaliza los identificadores codificados recortando a la longitud esperada
# y completando con ceros a la izquierda cuando corresponde.

for c, largo in padding_codigos_id.items():
    df_silver = df_silver.withColumn(
        c,
        F.when(
            F.col(c).isNotNull(),
            F.lpad(F.expr(f"right({c}, {largo})"), largo, "0")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Derivación de atributos (id_mesa, campo, registro)

# COMMAND ----------

# Construye un identificador único de mesa concatenando distrito, sección,
# circuito y mesa. Si alguno de estos componentes es nulo, se reemplaza por
# un placeholder de la longitud correspondiente.

df_silver = df_silver.withColumn(
    "id_mesa",
    F.concat(
        F.coalesce(F.col("distrito"), F.lit(placeholders_codigos_id["distrito"])),
        F.coalesce(F.col("seccion"), F.lit(placeholders_codigos_id["seccion"])),
        F.coalesce(F.col("circuito"), F.lit(placeholders_codigos_id["circuito"])),
        F.coalesce(F.col("mesa"), F.lit(placeholders_codigos_id["mesa"])),
        F.lit("X")
    )
)

# COMMAND ----------

# Construir el campo a partir del número de agrupación política y su denominación
df_silver = (
    df_silver
    .withColumn(
        "campo",
        F.concat_ws(" ", col("campo_numero"), col("campo_descripcion"))
    )
    .withColumn("campo", F.regexp_replace(col("campo"), r"[^\w\s]", ""))
    .withColumn("campo", F.regexp_replace(col("campo"), r"\s{2,}", " "))
    .withColumn("campo", F.trim(col("campo")))
)

# COMMAND ----------

#Castear columnas int 
df_silver = (
    df_silver
    .withColumn("cantidad", col("cantidad").cast("int"))
    .withColumn("inscriptos", col("inscriptos").cast("int"))
)

# COMMAND ----------

#Crear columna tipo de registro (Mesa o Agrupacion)
df_silver = df_silver.withColumn(
    "registro",
    F.when(
        F.col("campo_numero").cast("string").isin(list(codigos_mesa.values())),
        F.lit("MESA")
    ).otherwise(F.lit("AGRUPACION"))
)

# COMMAND ----------

#Identificar mesas especiales (compensatorias de votos recurridos, rere y privados)
# Criterios de clasificación:
# 1. COMPENSATORIA: la mesa contiene al menos un registro con votos negativos.
# 2. RESIDENTES_EXTERIOR:  la descripción de la sección
#    coincide con el patrón definido para residentes en el exterior.
# 3. PRIVADOS_LIBERTAD: la descripción de la sección
#    coincide con el patrón definido para personas privadas de la libertad.
# 4. NORMAL: cualquier mesa que no cumpla las reglas anteriores.

w = Window.partitionBy("id_mesa")
descripcion = F.upper(F.coalesce(F.col("seccion_descripcion"), F.lit("")))

df_silver = (
    df_silver

    # Indicadores a nivel de mesa
    .withColumn(
        "es_compensatoria",
        F.max(F.when(F.col("cantidad") < 0, 1).otherwise(0)).over(w)
    )
    .withColumn(
        "circuito_nulo",
        F.max(F.when(F.col("circuito").isNull(), 1).otherwise(0)).over(w)
    )
    .withColumn(
        "es_rere",
        F.max(F.when(descripcion.rlike(patron_rere), 1).otherwise(0)).over(w)
    )
    .withColumn(
        "es_pl",
        F.max(F.when(descripcion.rlike(patron_pl), 1).otherwise(0)).over(w)
    )

    # Clasificación del tipo de mesa
    .withColumn(
        "tipo_mesa",
        F.when(F.col("es_compensatoria") == 1, "COMPENSATORIA")
         .when((F.col("es_rere") == 1), "RESIDENTES_EXTERIOR")
         .when((F.col("es_pl") == 1), "PRIVADOS_LIBERTAD")
         .otherwise("NORMAL")
    )

    # Eliminar columnas auxiliares
    .drop(
        "es_compensatoria",
        "circuito_nulo",
        "es_rere",
        "es_pl"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Incorporación de fila de inscriptos

# COMMAND ----------

# Traer inscriptos de columnas a filas
df_inscriptos = (
    df_silver
    .filter(F.col("campo_numero") == codigos_mesa["votos_totales"])
    .select(
        "id_mesa",
        "distrito",
        "seccion",
        "seccion_descripcion",
        "circuito",
        "circuito_descripcion",
        "mesa",
        "categoria",
        F.lit(codigos_mesa["inscriptos"]).alias("campo_numero"),
        F.lit("INSCRIPTOS").alias("campo_descripcion"),
        F.col("inscriptos").alias("cantidad"),
        "campo",
        "registro",
        "tipo_mesa"
    )
)

# COMMAND ----------

#Reemplazar campo descripcion 
df_inscriptos = df_inscriptos.withColumn(
    "campo",
    F.concat_ws(" ", F.col("campo_numero"), F.col("campo_descripcion"))
)

# COMMAND ----------

# Completar nulls en cantidad
df_inscriptos = (
    df_inscriptos
    .withColumn("cantidad", F.coalesce(F.col("cantidad"), F.lit(0)))
)

# COMMAND ----------

# Eliminar la columna inscriptos del dataframe 
df_silver = df_silver.drop("inscriptos")

# COMMAND ----------

# Concatenar vertical los votos por mesas e inscriptos  
df_silver = df_silver.unionByName(df_inscriptos)

# COMMAND ----------

# MAGIC %md
# MAGIC Ordenamiento

# COMMAND ----------

# Ordenar columnas de tabla silver consolidada
df_silver = df_silver.select(*columnas_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Validaciones

# COMMAND ----------

# Validar reglas de calidad sobre un DataFrame, mostrando los registros inconsistentes y generando una excepción controlada cuando se detectan violaciones.


def validar_calidad(df, mensaje_error, columnas_mostrar=None):

    cantidad = df.count()

    print(f"{mensaje_error}: {cantidad}")

    if cantidad > 0:

        if columnas_mostrar:
            display(df.select(*columnas_mostrar))
        else:
            display(df)

        raise RuntimeError(
            f"{mensaje_error}. Se encontraron {cantidad} registro(s)."
        )

# COMMAND ----------

# Verificar que las mesas normales tengan completos los identificadores geográficos.
mesas_inconsistentes = (
    df_silver
    .filter(F.col("tipo_mesa") == "NORMAL")
    .filter(
        F.col("id_mesa").isNull() |
        F.col("distrito").isNull() |
        F.col("seccion").isNull() |
        F.col("circuito").isNull() |
        F.col("mesa").isNull()
    )
)

validar_calidad(
    df=mesas_inconsistentes,
    mensaje_error="Mesas normales con identificadores geográficos incompletos",
    columnas_mostrar=[
        "id_mesa",
        "distrito",
        "seccion",
        "circuito",
        "mesa",
        "tipo_mesa",
        "categoria",
        "campo_numero",
        "campo_descripcion",
        "cantidad"
    ]
)

# COMMAND ----------

# Verificar que la suma de cada mesa compensatoria sea igual a cero.
mesas_inconsistentes = (
    df_silver
    .filter(F.col("tipo_mesa") == "COMPENSATORIA")
    .groupBy("id_mesa")
    .agg(F.sum("cantidad").alias("total_votos"))
    .filter(F.col("total_votos") != 0)
)

validar_calidad(
    df=mesas_inconsistentes,
    mensaje_error="Mesas compensatorias con suma de votos distinta de cero"
)

# COMMAND ----------

# Verificar que todas las categorías pertenezcan al catálogo de categorías normalizadas.
categorias_invalidas = (
    df_silver
    .filter(~F.col("categoria").isin(*categorias_normalizadas))
    .select("categoria")
    .distinct()
)

validar_calidad(
    df=categorias_invalidas,
    mensaje_error="Categorías no válidas"
)

# COMMAND ----------

# Verificar que no existan valores nulos en votos.
votos_nulos = (
    df_silver
    .filter(F.col("cantidad").isNull())
)

validar_calidad(
    df=votos_nulos,
    mensaje_error="Registros con votos nulos",
    columnas_mostrar=[
        "id_mesa",
        "tipo_mesa",
        "categoria",
        "campo_numero",
        "campo_descripcion",
        "cantidad"
    ]
)

# COMMAND ----------

# Verificar que todos los identificadores de mesa tengan la longitud esperada.
id_mesa_invalidos = (
    df_silver
    .filter(F.length("id_mesa") != longitud_id_mesa)
)

validar_calidad(
    df=id_mesa_invalidos,
    mensaje_error="Identificadores de mesa con longitud inválida",
    columnas_mostrar=[
        "id_mesa",
        "tipo_mesa",
        "distrito",
        "seccion",
        "circuito",
        "mesa"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Persistencia de tabla consolidada

# COMMAND ----------

# Persistir silver consolidada
(
    df_silver
    .select(*spark.table(tabla_silver_consolidada).columns)
    .write
    .format("delta")
    .mode("overwrite")
    .insertInto(tabla_silver_consolidada)
)

# COMMAND ----------

# MAGIC %md
# MAGIC