# Proyecto 3 – Análisis de Clima y Movilidad con Spark y AWS EMR

# Información del curso
Materia: ST0263 - Tópicos Especiales en Telemática
Estudiantes:
Manuel Arango Gómez - marangog3@eafit.edu.co
Sebastián Cano Rincón - scanor2@eafit.edu.co
Maria Camila Morales - mcmorales@eafit.edu.co
Profesor: Alvaro Enrique Ospina Sanjuan - aeospinas@eafit.brightspace.com

## Nombre del proyecto

**Análisis de la relación entre condiciones climáticas y movilidad urbana en San Francisco**

---

## 1. Descripción de la actividad

Análisis cruzado entre datos climáticos históricos (Meteostat API) y datos de movilidad (Uber Movement) mediante arquitectura Big Data Batch sobre AWS.

### 1.1 Requerimientos cumplidos

- Captura automática de datos desde API (Meteostat) y archivos (Uber).
- Ingesta automática a S3 (zona raw).
- ETL automatizado en Spark sobre EMR (zona trusted).
- Análisis descriptivo con SparkSQL y SparkML (zona refined).
- Consulta de resultados por Athena y API Gateway.
- Automatización completa del pipeline con scripts.
## 2. Diseño de alto nivel / arquitectura

Arquitectura batch en AWS:
- **S3**: almacenamiento por zonas (raw, trusted, refined)
- **EMR + Spark**: procesamiento ETL y analítico
- **Athena + API Gateway**: consulta de resultados
- **Base de datos relacional simulada**: Postgres en EC2 o RDS

---

## 3. Ambiente de desarrollo

- Lenguaje: Python 3.11
- Librerías:
  - `boto3`, `pandas`, `requests`, `pyspark`
- Configuración:
  - `.env` para API keys y credenciales AWS
  - Scripts:
    - `ingest_meteostat.py`
    - `upload_uber.py`
    - `extract_db.py`
    - `etl_spark.py`
    - `analyze_spark.py`
    - `deploy_emr_cluster.py`
   
## 4. Ambiente de EJECUCIÓN (producción)

AWS S3, EMR, Athena, API Gateway

Configuración:

Bucket: s3://proyecto3-st0263-<usuario>/

Variables de entorno (.env)

Lanzamiento:

Ejecutar deploy/deploy_pipeline.sh

Guía de uso:

Subir datos → Lanzar Spark jobs → Consultar resultados

