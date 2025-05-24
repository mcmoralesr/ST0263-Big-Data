# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year

spark = SparkSession.builder.appName("ETLWeatherUber").getOrCreate()

# Cargar datos de S3
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/")
uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/uber/")

# Convertir columnas relevantes
weather_df = weather_df.withColumn("year", year(col("date")))

# Unir datasets por año (solo agregación básica como ejemplo)
weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")
uber_df = uber_df.withColumnRenamed("Year", "year")

# Unión (inner join por año)
joined_df = uber_df.join(weather_avg, on="year", how="inner")

# Guardar resultado a S3 en formato parquet
joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_uber/")

spark.stop()
