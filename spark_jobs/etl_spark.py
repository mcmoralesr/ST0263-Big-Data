# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, split, month, count as spark_count
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("ETLWeatherCovid").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

try:
    # Leer archivos desde carpetas
    weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/")
    covid_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/covid/")

    # Procesar weather
    weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    weather_df = weather_df.withColumn("year", year(col("date")))
    weather_df = weather_df.withColumn("tavg", col("tavg").cast("double"))
    weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

    # Procesar covid: tomar inicio del rango y extraer año
    covid_df = covid_df.filter(col("Date_of_Infection").isNotNull())
    covid_df = covid_df.withColumn("Date_of_Infection", to_date(col("Date_of_Infection"), "yyyy-MM-dd"))
    covid_df = covid_df.withColumn("year", year(col("Date_of_Infection")))
    

    covid_stats = covid_df.groupBy("year").agg(
        spark_count("Patient_ID").alias("total_cases"),
    )

    print("Estadisticas COVID por año: ")
    covid_stats.show()
    print("Temperatura promedio por año: ")
    weather_avg.show()

    # Join
    joined_df = covid_stats.join(weather_avg, on="year", how="inner")

    # Mostrar contenido
    print("=== Esquema del joined_df ===")
    joined_df.printSchema()

    print("=== Primeros registros del join ===")
    joined_df.show(10)

    print(f"Cantidad de filas: {joined_df.count()}")

    # Guardar en trusted
    output_path = "s3://proyecto3bigdata/trusted/joined_weather_covid/"
    joined_df.write.mode("overwrite").parquet(output_path)

    print("ETL completado con éxito")

except AnalysisException as ae:
    print(f"Error de análisis en Spark: {ae}")
except Exception as e:
    print(f"Error inesperado: {e}")
finally:
    spark.stop()

#columnas del covid: 
#COLUMNA 1: Patient_ID,Age,Gender,Region,Preexisting_Condition,Date_of_Infection,COVID_Strain,Symptoms,Severity,Hospitalized,Hospital_Admission_Date,Hospital_Discharge_Date,ICU_Admission,Ventilator_Support,Recovered,Date_of_Recovery,Reinfection,Date_of_Reinfection,Vaccination_Status,Vaccine_Type,Doses_Received,Date_of_Last_Dose,Long_COVID_Symptoms,Occupation,Smoking_Status,BMI
#COLUMNA 2: 1,69,Male,Hovedstaden,Obesity,2022-06-21,Delta,Mild,Moderate,Yes,2025-01-13,2025-01-26,No,No,Yes,2023-04-19,No,,Yes,None,1,2022-09-22,None,Healthcare,Never,27.7
#COLUMNA N...
