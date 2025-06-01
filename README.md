# Proyecto 3 – Análisis de Clima y COVID-19 con Spark y AWS EMR - BIG DATA

## Información del curso
Materia: ST0263 - Tópicos Especiales en Telemática  
Estudiantes:  
- Manuel Arango Gómez - marangog3@eafit.edu.co  
- Sebastián Cano Rincón - scanor2@eafit.edu.co  
- Maria Camila Morales - mcmorales@eafit.edu.co  
Profesor: Alvaro Enrique Ospina Sanjuan - aeospinas@eafit.brightspace.com

## Nombre del proyecto

**Análisis de la relación entre condiciones climáticas y contagios del COVID 19 en Dinamarca**

---

## 1. Descripción de la actividad

Análisis cruzado entre datos climáticos históricos (Meteostat API) y datos de contagiados por COVID 19 mediante arquitectura Big Data Batch sobre AWS.

### 1.1 Requerimientos cumplidos

- Captura automática de datos desde API (Meteostat) y archivos (COVID-19).
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
    - `upload_covid.py`
    - `extract_db.py`
    - `etl_spark.py`
    - `etl_spark_multi_year.py`
    - `refined_etl.py`
    - `validate_parquet.py`
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
cd ~/ST0263-Big-Data/spark_jobs

# Lanzar ETL simple
spark-submit --deploy-mode client etl_spark.py > etl_output.log 2>&1

# Lanzar ETL multi-año
spark-submit --deploy-mode client etl_spark_multi_year.py > multi_etl_output.log 2>&1

# Lanzar ETL refined
spark-submit --deploy-mode client refined_etl.py > refined_etl_output.log 2>&1

# Verificar resultados
aws s3 ls s3://proyecto3bigdata/trusted/joined_weather_covid/
aws s3 ls s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/
aws s3 ls s3://proyecto3bigdata/refined/covid_weather_summary/
```

![image](https://github.com/user-attachments/assets/164f1937-d84b-4f22-856e-1be76051b244)

![image](https://github.com/user-attachments/assets/ae870467-b1b0-454d-814f-92b42f6e3db4)


---

## 5. Resultados esperados

- Parquet resultantes en `trusted/` y `refined/`
- Dashboard consultable por Athena y scripts con Spark

5. Ejecución del ETL en EMR
Este script realiza la unión entre múltiples archivos históricos del clima (2022-2024) con los datos de contagiados por COVID-19 en Dinamarca. Guarda el resultado como Parquet en la zona trusted.

### Ejecución ETL Multi-Year

```bash
spark-submit etl_spark_multi_year.py
```

Salida esperada:

`s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/`

Validación con:

```bash
spark-submit validate_parquet.py
```  
![image](https://github.com/user-attachments/assets/8fed3349-9dba-48e1-8301-ce45599bc637)  
![image](https://github.com/user-attachments/assets/49b8af2d-dbbb-48b7-85d6-6c1d25c57850)





### Ejecución ETL Refined

```bash
spark-submit refined_etl.py
```

Salida esperada:

`s3://proyecto3bigdata/refined/covid_weather_summary/`

Archivo bootstrap

```bash
aws s3 cp s3://proyecto3bigdata/bootstrap/emr_bootstrap.sh .
```


---

## 6. Resumen de hallazgos

![image](https://github.com/user-attachments/assets/1a8a977f-b039-4a46-bef6-ffef9a37e9b9)



Esto indica que en condiciones climáticas frías, los casos de contagiados por COVID-19 aumentan más, esto se debe a que debido al frio, las gripes son más comunes, lo que disminuye las capacidades del sistema inmune, causando más probabilidad de contagiarse por otras enfermedades como el COVID-19.

---

## Comando de consulta en Athena

```sql
-- Promedio de temperatura y contagiados por covid-19 por año
SELECT
  year,
  ROUND(AVG(CAST("Mean Travel Time (Seconds)" AS DOUBLE)), 2) AS avg_travel_time,
  ROUND(AVG(avg_temp), 2) AS avg_temp
FROM
  proyecto3.joined_weather_uber_multiyear
GROUP BY
  year
ORDER BY
  year;
```

---

## Validación de resultados con PySpark (opcional)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ValidateParquet").getOrCreate()

df = spark.read.parquet("s3://proyecto3bigdata/trusted/joined_weather_uber_multiyear/")
print("Número total de filas:", df.count())
df.show(10)
df.printSchema()

spark.stop()
```

---

## Archivo origen

- **Tabla analizada**: `proyecto3.joined_weather_covid_multiyear`
- **Ubicación S3**: `s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/`
![image](https://github.com/user-attachments/assets/64434e62-55e0-41a3-903d-1255b59754cc)

## Resultados del analisis con SparkSQL + PipeLines
### Resumen
![image](https://github.com/user-attachments/assets/cd1d9991-8401-42f4-8ddc-2bb8316a06be)

### Analisis Descriptivo
![image](https://github.com/user-attachments/assets/93d5f339-6d1d-45b9-8278-0e95aaf6b9df)

### Análisis de Correlación
![image](https://github.com/user-attachments/assets/b4c45480-85f2-4380-bf40-937949338789)

### Análisis de Temperatura por Categoría
![image](https://github.com/user-attachments/assets/59db158d-7cc5-49c8-bdf9-6627ff502bbd)

Los archivos fueron guardados en S3 en refined/descriptive_analysis



## Resultados de predicciones con SparkML
El modelo se empezará a ejecutar:
![image](https://github.com/user-attachments/assets/2734a06f-eff6-46f3-ba31-1ad82dab2718)  
Este modelo busca predecir 4 variables importantes:
* El número de contagiados
* El numero de hospitalizados
* El número de casos con severidad igual a High
* El numero de casos de ICU (Clinical Cases in intensive care unit)

### Datos actuales:
![image](https://github.com/user-attachments/assets/7fbb61fd-e645-4985-b301-e33aca36524a)

### Total_case_model (Número de contagiados)
* RMSE: 74.62
* R2: 0.9641  
![image](https://github.com/user-attachments/assets/2aefcb0d-4651-49e9-922d-76ee5d648a53)

Todos los resultados fueron guardados en S3, más especificamente en s3a://proyecto3bigdata/refined/ml_analysis/*

### Hospitalized_model (Número de hospitalizados)
* RMSE: 25.36
* R2: 0.9544  
![image](https://github.com/user-attachments/assets/878463a5-d61f-48eb-b025-688398fb1bdf)

### severe_model (Casos severos altos)
* RMSE: 1.00
* R2: 0.9999  
![image](https://github.com/user-attachments/assets/c0329c94-c51e-4fac-9640-899965cb8c46)

### icu_model (Número de casos de ICU)
* RMSE: 9.49
* R2 : 0.8348  
![image](https://github.com/user-attachments/assets/f23a3ce3-26a8-4b59-acd9-021250949a52)


---
## API REST - Predicciones con Lambda + API Gateway

URL: https://9x473webmf.execute-api.us-east-1.amazonaws.com/prod/predictions

Parámetro: opcional ?year=2023

Ejemplo
* curl "https://9x473webmf.execute-api.us-east-1.amazonaws.com/prod/predictions?year=2023"

``` Respuests esperada:
[
  {
    "year": 2023,
    "total_cases": 1530,
    "hospitalized_cases": 453,
    "severe_cases": 380,
    "icu_cases": 87,
    "avg_temp": 9.995890410958902
  }
]
```

![image](https://github.com/user-attachments/assets/c3957a98-5d55-438b-9e1b-089524b76f7c)


## 6. Problemas encontrados

- Falta de permisos IAM para lanzar steps EMR

![image](https://github.com/user-attachments/assets/f686c2a5-a440-4762-9e17-0cfaeda02fb0)

- Solución: ejecución manual desde nodo maestro por SSH
