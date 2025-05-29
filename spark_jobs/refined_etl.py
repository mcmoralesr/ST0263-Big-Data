# spark_jobs/refined_etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, count as spark_count, when, round

spark = SparkSession.builder.appName("RefinedCovidWeatherETL").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Paths
TRUSTED_PARQUET = "s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/"
RAW_COVID = "s3://proyecto3bigdata/raw/covid19/"
OUTPUT_REFINED = "s3://proyecto3bigdata/refined/covid_weather_summary/"

# Load trusted data
trusted_df = spark.read.parquet(TRUSTED_PARQUET)

# Load raw covid data for regional info
covid_df = spark.read.option("header", True).csv(RAW_COVID)
covid_df = covid_df.withColumn("Date_of_Infection", to_date(col("Date_of_Infection"), "yyyy-MM-dd"))
covid_df = covid_df.withColumn("year", year(col("Date_of_Infection")))

# Filter invalid rows
covid_df = covid_df.filter(col("year").isNotNull() & col("Region").isNotNull())

# Aggregate by year and region
covid_by_region = covid_df.groupBy("year", "Region").agg(
    spark_count("Patient_ID").alias("total_cases"),
    spark_count(when(col("Hospitalized") == "Yes", 1)).alias("hospitalized"),
    spark_count(when(col("ICU_Admission") == "Yes", 1)).alias("icu"),
)

# Compute rates
covid_by_region = covid_by_region.withColumn(
    "pct_hospitalized", round((col("hospitalized") / col("total_cases")) * 100, 2)
).withColumn(
    "pct_icu", round((col("icu") / col("total_cases")) * 100, 2)
)

# Join with temp data (from trusted)
weather = trusted_df.select("year", "avg_temp").dropDuplicates()
summary_df = covid_by_region.join(weather, on="year", how="left")

# Output
summary_df.write.mode("overwrite").parquet(OUTPUT_REFINED)

print("✅ Refined ETL completo. Guardado en:", OUTPUT_REFINED)

spark.stop()
