# Resultados Electorales Pipeline

Pipeline de ingeniería de datos desarrollado en **Databricks** para procesar los resultados oficiales del escrutinio nacional argentino 2025.

El proyecto implementa un flujo **end-to-end** que transforma archivos planos provenientes de los 24 distritos electorales en un modelo analítico construido sobre **Delta Lake**, utilizando una arquitectura **Medallion (Bronze → Silver → Gold)**.

El objetivo principal fue diseñar un proceso reproducible, escalable y orientado a calidad de datos, capaz de convertir información heterogénea en datasets listos para consumo analítico.

---

# Objetivos

El pipeline fue diseñado para resolver problemas frecuentes presentes en procesos de integración de datos públicos:

- Automatizar la ingesta de múltiples archivos con un esquema común.
- Estandarizar identificadores provenientes de distintas jurisdicciones.
- Validar la consistencia estructural y de negocio antes de publicar información.
- Separar claramente las etapas de ingestión, transformación y consumo.
- Construir un modelo dimensional optimizado para análisis.

---

# Arquitectura

```text
TXT (24 distritos)
        │
        ▼
Bronze
(Ingesta)
        │
        ▼
Silver
(Limpieza + Normalización + Validaciones)
        │
        ▼
Gold
(Modelo dimensional)
        │
        ├── Fact Resultados
        ├── Dimensiones
        └── Data Mart Resumen Mesa
```
## Tecnologías utilizadas
La solución utiliza:

- Python
- PySpark
- SQL
- Databricks
- Delta Lake
- Unity Catalog
- Databricks Workflows

---

# Flujo del pipeline

## Bronze

Responsable de preservar los archivos originales minimizando las transformaciones.

Durante la carga se realizan únicamente operaciones necesarias para garantizar la correcta lectura:

- Esquema explícito.
- Lectura en modo `FAILFAST`.
- Eliminación de espacios.
- Conversión de `\N` a `NULL`.
- Almacenamiento en tablas Delta.

Al tratarse de datos históricos oficiales, la carga se realiza mediante **full refresh**, evitando la complejidad innecesaria de procesos incrementales.

---

## Silver

La capa Silver concentra la mayor parte de la lógica de ingeniería.

Aquí los datos son normalizados para construir un modelo consistente independientemente del distrito de origen.

Las principales transformaciones incluyen:

- Construcción del identificador único de mesa.
- Normalización de categorías electorales.
- Clasificación automática del tipo de mesa.
- Estandarización de claves geográficas.
- Consolidación de los 24 distritos en un único dataset.

Antes de persistir la información se ejecuta una batería de validaciones que detiene el pipeline ante inconsistencias.

Entre ellas:

- Categorías inválidas.
- Votos nulos.
- Identificadores incompletos.
- Mesas compensatorias con votos.
- Claves geográficas inconsistentes.

---

## Gold

La información se reorganiza mediante un modelo dimensional para facilitar el consumo analítico.

El modelo incluye:

- Tabla de hechos con resultados por mesa.
- Dimensiones jerárquicas de distrito, sección, circuito y mesa.
- Dimensiones de categorías y campos electorales.
- Un Data Mart desnormalizado con indicadores por mesa listo para herramientas de BI.

---

# Modelo de datos

Se implementó un esquema **Snowflake** para representar la estructura territorial del sistema electoral argentino.

```text
Distrito
    │
Sección
    │
Circuito
    │
Mesa
    │
Resultados
```

Esta decisión reduce redundancia en la información geográfica y mantiene explícitas las relaciones jerárquicas entre entidades.

---

# Calidad de datos

La calidad constituye un componente central del pipeline.

Antes de publicar la capa Silver se ejecutan validaciones automáticas que aseguran la consistencia de los datos.

Entre las principales verificaciones se incluyen:

- Integridad de claves.
- Completitud de identificadores.
- Consistencia de categorías.
- Reglas específicas para mesas especiales.
- Ausencia de cantidades nulas.

Si alguna validación falla, el proceso se interrumpe y expone los registros afectados para facilitar su diagnóstico.

---

# Organización del proyecto

```text
00_creacion_estructura
00_parametros
01_ingesta_bronze
02_transformacion_silver
03_modelado_gold
```

Cada notebook representa una etapa independiente del pipeline y puede ejecutarse de forma secuencial.

---

# Principales decisiones de diseño

- Arquitectura **Medallion** para desacoplar las etapas del procesamiento.
- **Delta Lake** como formato de almacenamiento transaccional.
- **Unity Catalog** para la administración centralizada de activos.
- Esquema explícito para evitar inferencias automáticas.
- **Full refresh** debido al carácter histórico e inmutable de la información.
- Validaciones de calidad antes de publicar datos.
- Modelo **Snowflake** para representar la jerarquía territorial.

 # Orquestación

La ejecución del pipeline se encuentra orquestada mediante **Databricks Workflows**, que coordina la ejecución secuencial de cada etapa del proceso:

1. Carga de parámetros.
2. Ingesta de datos en Bronze.
3. Transformación y validaciones en Silver.
4. Modelado dimensional en Gold.

La definición del workflow se encuentra versionada en el archivo `resultados_pipeline.yml`, permitiendo reproducir la orquestación del proceso como código
