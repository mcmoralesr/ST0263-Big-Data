# spark_jobs/etl_spark_multi_year.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

spark = SparkSession.builder.appName("ETLMultiYearWeatherUber").getOrCreate()

# Leer todos los CSV de Uber y Weather
uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/uber/*.csv")
weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/*.csv")

# Normalizar fechas y extraer año (asumimos formato YYYY-MM-DD)
uber_df = uber_df.withColumn("year", col("Year"))
weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
weather_df = weather_df.withColumn("year", year(col("date")))

# Promedio anual de temperatura
weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

# Unión por año
joined_df = uber_df.join(weather_avg, on="year", how="inner")

# Guardar resultado
joined_df.write.mode("overwrite").parquet("s3://proyecto3bigdata/trusted/joined_weather_uber/")

spark.stop()
