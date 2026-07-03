-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Creación de la Estructura del Pipeline de Resultados Electorales
-- MAGIC
-- MAGIC Este notebook define la estructura base del pipeline de resultados electorales en Unity Catalog, creando el catálogo y los schemas que organizan los datos según la arquitectura Medallion.
-- MAGIC
-- MAGIC **Objetivo**
-- MAGIC
-- MAGIC Establecer la estructura lógica sobre la cual se implementa el pipeline, separando los datos por capas de procesamiento para garantizar orden, trazabilidad y consistencia en la organización del proyecto.
-- MAGIC
-- MAGIC **Decisiones técnicas**
-- MAGIC
-- MAGIC - **Catálogo único.** Se utiliza un único catálogo para centralizar la administración de los objetos del proyecto y aislar el entorno de trabajo asociado al pipeline de resultados electorales.
-- MAGIC
-- MAGIC - **Separación por capas.** Dentro del catálogo, los datos se organizan en los schemas `bronze`, `silver` y `gold`, de acuerdo con la arquitectura Medallion y con el nivel de procesamiento esperado en cada etapa del pipeline.
-- MAGIC
-- MAGIC - **Alcance de cada capa.**
-- MAGIC   - **Bronze:** preserva los datos crudos provenientes de las fuentes de origen, con transformaciones mínimas de estandarización orientadas a la ingesta.
-- MAGIC   - **Silver:** concentra los datos limpios, tipificados y normalizados, listos para su integración y modelado posterior.
-- MAGIC   - **Gold:** contiene las estructuras analíticas orientadas al consumo, incluyendo tablas agregadas y modelos dimensionales bajo esquema estrella.
-- MAGIC
-- MAGIC **Alcance del notebook**
-- MAGIC
-- MAGIC Este notebook se limita a la creación de la estructura lógica del proyecto en Unity Catalog. No realiza procesos de ingesta ni transformaciones de datos, sino que establece el marco organizativo sobre el cual se apoyan los notebooks de las capas Bronze, Silver y Gold.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Configuración del entorno

-- COMMAND ----------

-- MAGIC %run Workspace/resultados-electorales-pipeline/00_parametros

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Organización del catalogo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear el catálogo y los esquemas del proyecto
-- MAGIC spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
-- MAGIC
-- MAGIC for schema in ["bronze", "silver", "gold"]:
-- MAGIC     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Capa bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear el volumen de la capa Bronze para almacenar los dumps de escrutinio
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC CREATE VOLUME IF NOT EXISTS escrutinio.bronze.raw_data_2025
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Crear las tablas Delta de la capa Bronze a partir del diseño de registro definido para la ingesta
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_bronze_resultados} (
-- MAGIC     {diseño_31_columnas_2025},
-- MAGIC     {columnas_auditoria_bronze}
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC PARTITIONED BY (dist_codigo)
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Capa Silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Crear la tabla Silver consolidada para almacenar los resultados de los 24 distritos
-- MAGIC ## Definir la estructura de salida con nombres de columnas alineados a la lógica de negocio
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_silver_consolidada} (
-- MAGIC     id_mesa STRING NOT NULL,
-- MAGIC     distrito STRING NOT NULL,
-- MAGIC     seccion STRING,
-- MAGIC     seccion_descripcion STRING,
-- MAGIC     circuito STRING,
-- MAGIC     circuito_descripcion STRING,
-- MAGIC     mesa STRING,
-- MAGIC     categoria STRING NOT NULL,
-- MAGIC     campo STRING,
-- MAGIC     campo_numero STRING,
-- MAGIC     campo_descripcion STRING,
-- MAGIC     cantidad INT,
-- MAGIC     registro STRING,
-- MAGIC     tipo_mesa STRING
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Capa gold 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Distrito
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_dim_distrito} (
-- MAGIC     id_distrito STRING NOT NULL,
-- MAGIC     distrito STRING NOT NULL,
-- MAGIC     CONSTRAINT pk_g2025_dim_distrito PRIMARY KEY (id_distrito)
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Seccion 
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_dim_seccion} (
-- MAGIC     id_seccion STRING NOT NULL,
-- MAGIC     id_distrito STRING NOT NULL,
-- MAGIC     seccion_numero STRING NOT NULL,
-- MAGIC     seccion_descripcion STRING,
-- MAGIC     CONSTRAINT pk_g2025_dim_seccion PRIMARY KEY (id_seccion),
-- MAGIC     CONSTRAINT fk_g2025_dim_seccion_distrito
-- MAGIC     FOREIGN KEY (id_distrito)
-- MAGIC     REFERENCES {tabla_gold_dim_distrito}(id_distrito)
-- MAGIC
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Circuito
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_dim_circuito} (
-- MAGIC     id_circuito STRING NOT NULL,
-- MAGIC     id_seccion STRING NOT NULL,
-- MAGIC     circuito_numero STRING,
-- MAGIC     circuito_descripcion STRING,
-- MAGIC     CONSTRAINT pk_g2025_dim_circuito PRIMARY KEY (id_circuito),
-- MAGIC     CONSTRAINT fk_g2025_dim_circuito_seccion
-- MAGIC     FOREIGN KEY (id_seccion)
-- MAGIC     REFERENCES {tabla_gold_dim_seccion}(id_seccion)
-- MAGIC
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Mesa
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TABLE {tabla_gold_dim_mesa} (
-- MAGIC     id_mesa STRING NOT NULL,
-- MAGIC     id_circuito STRING NOT NULL,
-- MAGIC     mesa STRING NOT NULL,
-- MAGIC     tipo_mesa STRING NOT NULL,
-- MAGIC     CONSTRAINT pk_g2025_dim_mesa PRIMARY KEY (id_mesa),
-- MAGIC     CONSTRAINT fk_g2025_dim_mesa_circuito
-- MAGIC         FOREIGN KEY (id_circuito)
-- MAGIC         REFERENCES {tabla_gold_dim_circuito}(id_circuito)
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Categoria 
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_dim_categoria} (
-- MAGIC     id_categoria STRING NOT NULL,
-- MAGIC     codigo STRING NOT NULL,
-- MAGIC     CONSTRAINT pk_g2025_dim_categoria PRIMARY KEY (id_categoria)
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Campo
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_dim_campo} (
-- MAGIC     id_campo STRING NOT NULL,
-- MAGIC     id_distrito STRING NOT NULL,
-- MAGIC     id_categoria STRING NOT NULL,
-- MAGIC     campo_numero STRING,
-- MAGIC     campo_descripcion STRING NOT NULL,
-- MAGIC     CONSTRAINT pk_g2025_dim_campo PRIMARY KEY (
-- MAGIC         id_campo
-- MAGIC     )
-- MAGIC
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear las tablas de hechos
-- MAGIC # La tabla de hechos almacena los votos por agrupación electoral y mesa
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_fact_resultados} (
-- MAGIC     id_mesa STRING NOT NULL,
-- MAGIC     id_campo STRING NOT NULL,
-- MAGIC     cantidad INT NOT NULL,
-- MAGIC
-- MAGIC     CONSTRAINT pk_g2025_fact_resultados PRIMARY KEY (
-- MAGIC         id_mesa,
-- MAGIC         id_campo
-- MAGIC     ),
-- MAGIC
-- MAGIC     CONSTRAINT fk_g2025_fact_mesa
-- MAGIC         FOREIGN KEY (id_mesa)
-- MAGIC         REFERENCES {tabla_gold_dim_mesa}(id_mesa),
-- MAGIC
-- MAGIC     CONSTRAINT fk_g2025_fact_campo
-- MAGIC         FOREIGN KEY (id_campo)
-- MAGIC         REFERENCES {tabla_gold_dim_campo}(id_campo)
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Votos agregados por mesa
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {tabla_gold_resumen_mesa} (
-- MAGIC     categoria STRING NOT NULL,
-- MAGIC     id_mesa STRING NOT NULL,
-- MAGIC     distrito STRING NOT NULL,
-- MAGIC     seccion STRING,
-- MAGIC     seccion_descripcion STRING,
-- MAGIC     circuito STRING,
-- MAGIC     circuito_descripcion STRING,
-- MAGIC     mesa STRING,
-- MAGIC     tipo_mesa STRING,
-- MAGIC     listas BIGINT,
-- MAGIC     inscriptos INT,
-- MAGIC     votos_afirmativos BIGINT,
-- MAGIC     votos_blanco INT,
-- MAGIC     votos_nulos INT,
-- MAGIC     votos_recurridos INT,
-- MAGIC     votos_impugnados INT,
-- MAGIC     votos_totales INT,
-- MAGIC     CONSTRAINT pk_g2025_resumen_mesa
-- MAGIC     PRIMARY KEY (categoria, id_mesa)
-- MAGIC )
-- MAGIC USING DELTA;
-- MAGIC """)