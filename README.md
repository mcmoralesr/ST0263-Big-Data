# Proyecto 3 – Análisis de Clima y Movilidad con Spark y AWS EMR - BIG DATA

## Información del curso
Materia: ST0263 - Tópicos Especiales en Telemática  
Estudiantes:  
- Manuel Arango Gómez - marangog3@eafit.edu.co  
- Sebastián Cano Rincón - scanor2@eafit.edu.co  
- Maria Camila Morales - mcmorales@eafit.edu.co  
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

![image](https://github.com/user-attachments/assets/de184001-a6d6-4ed0-9a3d-f1ef8ba864ad)


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
    - `etl_spark_multi_year.py`
    - `analyze_spark.py`
    - `deploy_emr_cluster.py`

## 4. Ambiente de EJECUCIÓN (producción)

Infraestructura:
- AWS S3, EMR, Athena, API Gateway

Configuración:
- Bucket: `s3://proyecto3bigdata/`
- Variables de entorno: `.env`

Guía de ejecución en el nodo principal EMR:

```bash
# Entrar por SSH
ssh -i ~/labsuser.pem hadoop@<master-public-dns>

# Ir al repo local
cd ~/etl_run/ST0263-Big-Data/spark_jobs

# Lanzar ETL simple
spark-submit --deploy-mode client etl_spark.py > etl_output.log 2>&1

# Lanzar ETL multi-año
spark-submit --deploy-mode client etl_spark_multi_year.py > multi_etl_output.log 2>&1

# Verificar resultados
aws s3 ls s3://proyecto3bigdata/trusted/joined_weather_uber/
aws s3 ls s3://proyecto3bigdata/trusted/joined_weather_uber_multiyear/
```

---

## 5. Resultados esperados

- Parquet resultantes en `trusted/` con join de datos Uber + Meteo
- Dashboards consultables vía Athena y/o scripts con Spark

## 6. Problemas encontrados

- Falta de permisos IAM para lanzar steps EMR

![image](https://github.com/user-attachments/assets/f686c2a5-a440-4762-9e17-0cfaeda02fb0)

- Solución: ejecución manual desde nodo maestro por SSH
