# spark_jobs/etl_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, split
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("ETLWeatherUber").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

try:
    # Leer múltiples archivos por año
    weather_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/weather/")
    uber_df = spark.read.option("header", True).csv("s3://proyecto3bigdata/raw/uber/")

    # Procesar weather
    weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    weather_df = weather_df.withColumn("year", year(col("date")))
    weather_df = weather_df.withColumn("tavg", col("tavg").cast("double"))
    weather_avg = weather_df.groupBy("year").avg("tavg").withColumnRenamed("avg(tavg)", "avg_temp")

    # Procesar uber: tomar inicio del rango y extraer año
    uber_df = uber_df.withColumn("date_range", split(col("Date Range"), " ").getItem(0))
    uber_df = uber_df.withColumn("date_range", to_date(col("date_range"), "yyyy-MM-dd"))
    uber_df = uber_df.withColumn("year", year(col("date_range")))

    # Join por año
    joined_df = uber_df.join(weather_avg, on="year", how="inner")

    print("=== Esquema del joined_df ===")
    joined_df.printSchema()

    print("=== Primeros registros del join ===")
    joined_df.show(10)

    print(f"Cantidad de filas: {joined_df.count()}")

    # Guardar en carpeta multianual trusted
    output_path = "s3://proyecto3bigdata/trusted/joined_weather_uber_multiyear/"
    joined_df.write.mode("overwrite").parquet(output_path)

    print("ETL multiyear completado con éxito")

except AnalysisException as ae:
    print(f"Error de análisis en Spark: {ae}")
except Exception as e:
    print(f"Error inesperado: {e}")
finally:
    spark.stop()
