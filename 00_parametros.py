# Databricks notebook source
# Parámetros del pipeline de resultados electorales.
# Define configuraciones compartidas para ser reutilizadas por los notebooks del proyecto y facilitar su mantenimiento.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Definir catálogo del proyecto
catalogo = "escrutinio"

# Definir rutas
ruta_raw_data = f"/Volumes/{catalogo}/bronze/raw_data_2025"

# Tablas Bronze
tabla_bronze_resultados = f"{catalogo}.bronze.g2025_resultados"

# Tabla Silver
tabla_silver_consolidada = f"{catalogo}.silver.g2025_resultados_consolidado"

# Tablas Gold (Snowflake Schema)
# Fact
tabla_gold_fact_resultados = f"{catalogo}.gold.g2025_fact_resultados"

# Dimensiones
tabla_gold_dim_mesa = f"{catalogo}.gold.g2025_dim_mesa"
tabla_gold_dim_circuito = f"{catalogo}.gold.g2025_dim_circuito"
tabla_gold_dim_seccion = f"{catalogo}.gold.g2025_dim_seccion"
tabla_gold_dim_distrito = f"{catalogo}.gold.g2025_dim_distrito"
tabla_gold_dim_categoria = f"{catalogo}.gold.g2025_dim_categoria"
tabla_gold_dim_campo = f"{catalogo}.gold.g2025_dim_campo"

# Data mart
tabla_gold_resumen_mesa = f"{catalogo}.gold.g2025_resumen_mesa"
 

# COMMAND ----------

# Definir la serie de distritos electorales con formato de dos dígitos ('01' a '24')
distritos = [f"{i:02d}" for i in range(1, 25)]

# COMMAND ----------


# Definir el diccionario de equivalencias entre código electoral y nombre del distrito

iddistritos = {
    "01": "CAPITAL_FEDERAL", 
    "02": "BUENOS_AIRES", 
    "03": "CATAMARCA", 
    "04": "CORDOBA",
    "05": "CORRIENTES", 
    "06": "CHACO", 
    "07": "CHUBUT", 
    "08": "ENTRE_RIOS",
    "09": "FORMOSA", 
    "10": "JUJUY", 
    "11": "LA_PAMPA", 
    "12": "LA_RIOJA",
    "13": "MENDOZA", 
    "14": "MISIONES", 
    "15": "NEUQUEN", 
    "16": "RIO_NEGRO",
    "17": "SALTA", 
    "18": "SAN_JUAN", 
    "19": "SAN_LUIS", 
    "20": "SANTA_CRUZ",
    "21": "SANTA_FE", 
    "22": "SANTIAGO_DEL_ESTERO", 
    "23": "TIERRA_DEL_FUEGO", 
    "24": "TUCUMAN"}

# COMMAND ----------

# Definir el esquema de 31 columnas para los archivos de escrutinio 2025
diseño_31_columnas_2025 = """
    dist_codigo STRING, zona STRING, zona_codigo STRING, zona_descrip STRING,
    secg_codigo STRING, secg_descrip STRING, secc_codigo STRING, secc_descrip STRING,
    muni_codigo STRING, muni_descrip STRING, circ_codigo STRING, circ_descrip STRING,
    mes_sexo STRING, tmesa_codigo STRING, ppar_codigo STRING, ppar_descrip STRING,
    ppar_codvis STRING, ppar_codvis2 STRING, ppar_codvis3 STRING, ppar_orden STRING,
    ppar_vercodi STRING, ppar_subcod STRING, ppar_subcod_descrip STRING, carg_codigo STRING,
    carg_descrip STRING, carg_abrev1 STRING, carg_orden STRING, cant STRING,
    totins STRING, totvot STRING, sobcub STRING
"""

# COMMAND ----------

# Extraer los nombres de los campos definidos en el diseño de registro 2025
columnas_2025 = [
    campo.strip().split()[0]
    for campo in diseño_31_columnas_2025.strip().split(",")]

# COMMAND ----------

# Definir esquema de entrada
schema_2025 = StructType([
    StructField(c, StringType(), True)
    for c in columnas_2025
])

# COMMAND ----------

# Definir esquema de auditoria

columnas_auditoria_bronze = """
_source_file STRING,
_ingestion_timestamp TIMESTAMP
"""

# COMMAND ----------

#Definir categorias de cargos electivos normalizadas
categorias_normalizadas = ["PV",	"DN",	"SN",	"PN",	"PR",	"GO",	"DP",	"DD",	"SP",	"MU",	"CO",	"TC",	"RP", "CC",
"JU"]


# COMMAND ----------

#Establecer reglas de padding id_mesa
padding_codigos_id = { "distrito": 2, "seccion": 3, "circuito": 5, "mesa": 5, "campo_numero": 3}
placeholders_codigos_id = {"distrito": "NN", "seccion": "NNN", "circuito": "NNNNN", "mesa": "NNNNN"}

longitud_id_mesa = (
    padding_codigos_id["distrito"] +
    padding_codigos_id["seccion"] +
    padding_codigos_id["circuito"] +
    padding_codigos_id["mesa"] 
    + 1  # Sufijo "X"
)

# COMMAND ----------

#Definir codigos de totales de mesa y votos especiales
codigos_mesa = {
    "inscriptos": "000",
    "votos_blanco": "990",
    "votos_nulos": "991",
    "votos_recurridos": "992",
    "votos_impugnados": "993",
    "votos_totales": "994"}




# COMMAND ----------

# Patrones utilizados para identificar mesas especiales a partir de la descripción
# de la sección. Se emplean raíces de palabras para contemplar distintas variantes
# de escritura presentes en los archivos fuente.

patron_rere = "EXTERIOR|RESIDENT|ARGENT"
patron_pl = "PRIV|LIBERTAD"

# COMMAND ----------

print("Parámetros del pipeline cargados exitosamente.")