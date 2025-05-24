# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, substring

spark = SparkSession.builder.appName("ETLWeatherUber").getOrCreate()

# Cargar datos de S3
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/")
uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/uber/")

# Convertir columnas relevantes
weather_df = weather_df.withColumn("year", year(to_date(col("date"), "yyyy-MM-dd")))

# Extraer año desde Date Range en Uber
uber_df = uber_df.withColumn("year", substring(col("Date Range"), 1, 4).cast("int"))

# Agregación por año
weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

# Unión por año
joined_df = uber_df.join(weather_avg, on="year", how="inner")

# Guardar resultado a S3 en formato parquet
joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_uber/")

spark.stop()
