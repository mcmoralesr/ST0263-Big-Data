# etl_spark_multiyear.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, regexp_extract, avg
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("ETLMultiYearWeatherUber").getOrCreate()

# Leer todos los archivos de clima
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/*.csv")

# Limpiar y convertir columnas
weather_df = weather_df \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("tavg", col("tavg").cast(DoubleType())) \
    .withColumn("year", year(col("date")))

# Filtrar valores v  lidos
weather_df = weather_df.filter(col("tavg").isNotNull())

# Promedio anual de temperatura
weather_avg = weather_df.groupBy("year").agg(avg("tavg").alias("avg_temp"))

# Leer archivo Uber
uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/uber/Travel_Times.csv")

# Extraer a  o de rango de fechas con RegEx
uber_df = uber_df.withColumn("date_range", regexp_extract(col("Date Range"), r"(\d{4})", 1))
uber_df = uber_df.withColumn("year", col("date_range").cast("int"))

# Join
joined_df = uber_df.join(weather_avg, on="year", how="inner")

print("Esquema del resultado:")
joined_df.printSchema()

print("Ejemplo de datos:")
joined_df.show(5)

row_count = joined_df.count()
print("Total de filas a escribir:", row_count)

# Guardar a S3 si hay datos
if row_count > 0:
    joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_uber_multiyear/")
    print(" ^|^e Parquet guardado exitosamente.")
else:
    print(" ^z   ^o No hay datos para guardar.")

spark.stop()
