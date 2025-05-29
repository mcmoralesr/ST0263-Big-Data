# etl_spark_multiyear.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, avg, count as spark_count, when
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("ETLMultiYearWeatherCovid").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Leer todos los archivos de clima
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/*.csv")

# Limpiar y convertir columnas
weather_df = weather_df \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("tavg", col("tavg").cast(DoubleType())) \
    .withColumn("year", year(col("date")))

# Filtrar valores v  lidos
weather_df = weather_df.filter(col("tavg").isNotNull() & col("year").isNotNull())

# Promedio anual de temperatura
weather_avg = weather_df.groupBy("year").agg(avg("tavg").alias("avg_temp"))

# Leer archivo COVID
covid_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/covid19/*.csv")

# Datos COVID19
covid_df = covid_df.filter(col("Date_of_Infection").isNotNull())
covid_df = covid_df.withColumn("Date_of_Infection", to_date(col("Date_of_Infection"), "yyyy-MM-dd"))
covid_df = covid_df.withColumn("year", year(col("Date_of_Infection")))
covid_df = covid_df.filter(col("year").isNotNull())

#Estadisticas COVID por año
covid_stats = covid_df.groupBy("year").agg(
    spark_count("Patient_ID").alias("total_cases"),
    spark_count(when(col("Hospitalized") == "Yes", 1)).alias("hospitalized_cases"),
    spark_count(when(col("Severity") == "Severe", 1)).alias("severe_cases"),
    spark_count(when(col("ICU_Admission") == "Yes", 1)).alias("icu_cases")
)

print("Estadisticas COVID 19 por año: ")
covid_stats.orderBy("year").show()

# Join entre COVID y clima
joined_df = covid_stats.join(weather_avg, on="year", how="inner")

print("Esquema del resultado:")
joined_df.printSchema()

print("Ejemplo de datos:")
joined_df.show(5)

row_count = joined_df.count()
print(f"Total de filas a escribir > {row_count}")

# Guardar a S3 si hay datos
if row_count > 0:
    joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/")
    print("Parquet guardado exitosamente.")
else:
    print("No hay datos para guardar.")

spark.stop()
