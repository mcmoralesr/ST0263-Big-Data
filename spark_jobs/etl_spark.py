# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

spark = SparkSession.builder.appName("ETLWeatherUber").getOrCreate()

# Cargar datos de S3
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/sf_weather_2009.csv")
uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/Travel_Times.csv")

# Asegurar que la columna "date" sea de tipo fecha
weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
weather_df = weather_df.withColumn("year", year(col("date")))

# Agrupar por año y calcular temperatura promedio
weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

# Renombrar columna Year en uber_df a year
uber_df = uber_df.withColumnRenamed("Year", "year")

# Unión interna por año
joined_df = uber_df.join(weather_avg, on="year", how="inner")

# Guardar resultado a S3
joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_uber/")

spark.stop()
