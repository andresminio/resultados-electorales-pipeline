# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesta de Resultados Electorales en Bronze
# MAGIC
# MAGIC Este notebook implementa el proceso de ingesta de los archivos de escrutinio 2025 en formato `.txt`, correspondientes a los 24 distritos electorales. A partir de los archivos fuente almacenados en un volumen de Databricks, realiza la lectura, validación estructural, estandarización y carga de los datos, persistiendo el resultado en una única tabla Delta de la capa Bronze de la arquitectura Medallion.
# MAGIC
# MAGIC Como resultado, se genera una tabla Delta particionada por `dist_codigo` que preserva la estructura y el contenido de los datos de origen, constituyendo la base para las transformaciones posteriores de la capa Silver.
# MAGIC
# MAGIC ### Decisiones técnicas
# MAGIC
# MAGIC #### Procesamiento batch
# MAGIC
# MAGIC La ingesta se ejecuta mediante una carga completa del conjunto de archivos fuente. En cada ejecución se reemplaza íntegramente el contenido de la tabla Bronze y se vuelven a cargar los registros disponibles, garantizando la idempotencia del proceso y asegurando que la información almacenada refleje la última versión disponible de los archivos de origen.
# MAGIC
# MAGIC El proceso incorpora manejo de errores por distrito. Ante una falla durante la lectura o validación de un archivo, el error se registra, el procesamiento continúa con los distritos restantes y el notebook finaliza con estado de error si se detectó al menos una falla, permitiendo su correcta detección por mecanismos de monitoreo u orquestación.
# MAGIC
# MAGIC #### Esquema de entrada
# MAGIC
# MAGIC La lectura se realiza utilizando un esquema (`StructType`) explícito de 31 columnas de tipo `STRING`, definido previamente en los parámetros del proyecto. Esta estrategia documenta formalmente la estructura esperada del archivo de origen y desacopla la definición del esquema de las transformaciones posteriores, delegando la tipificación, validación semántica y aplicación de reglas de negocio a la capa Silver.
# MAGIC
# MAGIC #### Validación de archivos fuente
# MAGIC
# MAGIC La lectura de los archivos se realiza utilizando el modo `FAILFAST` de Spark. Esta configuración interrumpe inmediatamente la ingesta cuando se detecta un registro estructuralmente inválido durante el parseo del archivo, evitando continuar el procesamiento sobre un archivo potencialmente corrupto.
# MAGIC
# MAGIC Dado que se trata de resultados oficiales de escrutinio, se privilegia la integridad de los datos por sobre la tolerancia a registros malformados, evitando que errores de formato pasen inadvertidos durante la carga.
# MAGIC
# MAGIC #### Modelado de la capa Bronze
# MAGIC
# MAGIC La información se almacena en una única tabla Delta particionada por `dist_codigo`, siguiendo el patrón recomendado por la arquitectura Medallion. La partición optimiza las consultas filtradas por distrito mediante *partition pruning*, manteniendo un único objeto de datos que simplifica la gobernanza, el linaje y la administración del catálogo.
# MAGIC
# MAGIC #### Alcance de la capa Bronze
# MAGIC
# MAGIC La capa Bronze preserva una representación estandarizada de los archivos fuente. No se aplican reglas de negocio ni transformaciones analíticas, priorizando la conservación de los datos originales, la trazabilidad del proceso de carga y la fidelidad respecto del sistema de origen.
# MAGIC
# MAGIC ### Transformaciones aplicadas
# MAGIC
# MAGIC Durante la ingesta se realizan únicamente transformaciones básicas de estandarización:
# MAGIC
# MAGIC - Lectura utilizando codificación **ISO-8859-1** para preservar caracteres especiales.
# MAGIC - Aplicación de un esquema explícito de 31 columnas durante la lectura.
# MAGIC - Validación estructural del archivo mediante el modo `FAILFAST`.
# MAGIC - Conversión de valores `\N` a `NULL`.
# MAGIC - Eliminación de espacios en blanco al inicio y al final de cada campo mediante `trim`.
# MAGIC
# MAGIC ### Persistencia
# MAGIC
# MAGIC Los registros se almacenan en una tabla Delta de la capa Bronze mediante una estrategia de carga completa. En cada ejecución, el contenido existente de la tabla se elimina y los datos válidos se vuelven a cargar utilizando inserciones en modo `append`.
# MAGIC
# MAGIC Esta estrategia garantiza la idempotencia del pipeline y resulta adecuada para un conjunto de datos histórico e inmutable como los resultados oficiales de escrutinio, donde no se esperan cargas incrementales sino, eventualmente, el reemplazo completo de los archivos fuente.
# MAGIC
# MAGIC ### Output principal
# MAGIC
# MAGIC Tabla Delta de la capa Bronze, particionada por `dist_codigo`, que contiene la totalidad de los resultados de escrutinio preservando la estructura y representación de los archivos de origen.
# MAGIC
# MAGIC **`G2025_resultados`**
# MAGIC

# COMMAND ----------

# Importar librerías
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %run Workspace/resultados-electorales-pipeline/00_parametros

# COMMAND ----------

# Inicializar el registro de errores
errores = []

# COMMAND ----------

# Limpiar la tabla Bronze antes de la carga completa
spark.sql(f"TRUNCATE TABLE {tabla_bronze_resultados}")

# COMMAND ----------

# Ingerir archivos de escrutinio por distrito
for distrito in distritos:
    try:

        # Leer archivo fuente
        df = (
            spark.read
            .schema(schema_2025)
            .option("sep", "|")
            .option("header", "false")
            .option("encoding", "ISO-8859-1")
            .option("mode", "FAILFAST")
            .csv(f"{ruta_raw_data}/2025 - {distrito} - RESULTADOS.txt")
            .withColumn(
                "_source_file",
                F.lit(f"{ruta_raw_data}/2025 - {distrito} - RESULTADOS.txt")
            )
            .withColumn(
                "_ingestion_timestamp",
                F.current_timestamp()
            )
        )
        
        # Aplicar transformaciones de limpieza
        df = df.select(
            *[
                F.when(F.trim(F.col(c)) == "\\N", None)
                .otherwise(F.trim(F.col(c)))
                .alias(c)
                for c in df.columns
            ]
            )

        # Persistir registros en Bronze
        (
            df.write
            .format("delta")
            .mode("append")
            .insertInto(f"{tabla_bronze_resultados}")
        )

        print(
            f"✔️ Distrito {distrito} procesado correctamente "
            f"({df.count():,} registros agregados)"
        )

    except Exception as e:
        errores.append(
            {
                "distrito": distrito,
                "error": str(e)
            }
        )

        print(f" Error procesando el distrito {distrito}: {e}. No se incorpora a la tabla")

# COMMAND ----------

# Resumen final de errores
if errores:
    print("\nSe produjeron errores durante la ingesta:\n")
    for error in errores:
        print(f"- {error['distrito']}: {error['error']}")

    raise RuntimeError(
        f"La ingesta finalizó con errores en {len(errores)} distrito(s)."
    )

print("\n✅ Ingesta Bronze finalizada correctamente.")