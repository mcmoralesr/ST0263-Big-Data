from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ValidateParquet").getOrCreate()

# Leer el Parquet
df = spark.read.parquet("s3://proyecto3bigdata/trusted/joined_weather_uber_multiyear/")

# Mostrar cantidad de registros
print("N  mero de filas:", df.count())

# Mostrar algunas filas
df.show(10)

# Mostrar columnas y esquema
df.printSchema()

spark.stop()

