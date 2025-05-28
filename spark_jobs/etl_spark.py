# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, when, count as spark_count
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("ETLWeatherCovid").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

try:
    # Leer archivos desde carpetas
    weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/")
    covid_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/covid/")

    print("=== Datos iniciales cargados ===")
    print(f"Weather records: {weather_df.count()}")
    print(f"COVID records: {covid_df.count()}")

    # Procesar weather
    weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    weather_df = weather_df.withColumn("year", year(col("date")))
    weather_df = weather_df.withColumn("tavg", col("tavg").cast("double"))
    weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

    # Procesar COVID
    covid_df = covid_df.filter(col("Date_of_Infection").isNotNull())
    covid_df = covid_df.withColumn("Date_of_Infection", to_date(col("Date_of_Infection"), "yyyy-MM-dd"))
    covid_df = covid_df.withColumn("year", year(col("Date_of_Infection")))
    covid_df = covid_df.filter(col("year").isNotNull())

    # Estadísticas COVID por año
    covid_stats = covid_df.groupBy("year").agg(
        spark_count("Patient_ID").alias("total_cases"),
        spark_count(when(col("Hospitalized") == "Yes", 1)).alias("hospitalized_cases"),
        spark_count(when(col("Severity") == "Severe", 1)).alias("severe_cases"),
        spark_count(when(col("ICU_Admission") == "Yes", 1)).alias("icu_cases")
    )

    print("=== Estadísticas COVID por año ===")
    covid_stats.orderBy("year").show()
    
    print("=== Temperatura promedio por año ===")
    weather_avg.orderBy("year").show()

    # Join por año
    joined_df = covid_stats.join(weather_avg, on="year", how="inner")

    # Mostrar contenido final
    print("=== Esquema del dataset final ===")
    joined_df.printSchema()

    print("=== Dataset final: COVID vs Clima ===")
    joined_df.orderBy("year").show()

    print(f"Total de años con datos completos: {joined_df.count()}")

    # Guardar en trusted
    output_path = "s3://proyecto3bigdata/trusted/joined_weather_covid/"
    joined_df.write.mode("overwrite").parquet(output_path)

    print("ETL completado con éxito - Datos guardados en:", output_path)

except AnalysisException as ae:
    print(f"Error de análisis en Spark: {ae}")
except Exception as e:
    print(f"Error inesperado: {e}")
finally:
    spark.stop()

#columnas del covid: 
#COLUMNA 1: Patient_ID,Age,Gender,Region,Preexisting_Condition,Date_of_Infection,COVID_Strain,Symptoms,Severity,Hospitalized,Hospital_Admission_Date,Hospital_Discharge_Date,ICU_Admission,Ventilator_Support,Recovered,Date_of_Recovery,Reinfection,Date_of_Reinfection,Vaccination_Status,Vaccine_Type,Doses_Received,Date_of_Last_Dose,Long_COVID_Symptoms,Occupation,Smoking_Status,BMI
#COLUMNA 2: 1,69,Male,Huvudstaden,Obesity,2022-06-21,Delta,Mild,Moderate,Yes,2025-01-13,2025-01-26,No,No,Yes,2023-04-19,No,,Yes,None,Healthcare,Never,27.7