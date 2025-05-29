from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, stddev, corr, round as spark_round, count, sum as spark_sum

def create_spark_session():
    return SparkSession.builder \
        .appName("Descriptive Analysis of COVID-CLIMATE") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def load_data(spark):
    try:
        df = spark.read.parquet("s3://proyecto3bigdata/trusted/joined_weather_covid_multiyear/")
        print("Datos cargados correctamente.")
        print("Imprimiendo esquema de datos...")
        df.printSchema()
        print("Mostrando los primeros 5 datos...")
        df.show(5)
        return df
    except Exception as e:
        print("Error al cargar los datos: ", e)
        raise

def basic_statistics_analysis(spark, df):
    print("\n" + "="*50)
    print("Analisis COVID-CLIMA")
    print("="*50 + "\n")
    df.createOrReplaceTempView("covid_climate")
    print("Analizando datos...")
    basic_stats = spark.sql("""
        SELECT
            year,
            total_cases,
            hospitalized_cases,
            severe_cases,
            icu_cases,
            ROUND(avg_temp, 2) as avg_temp_celsius,
            ROUND(hospitalized_cases * 100.0 / total_cases, 2) as hospitalization_rate,
            ROUND(severe_cases * 100.0 / total_cases, 2) as severity_rate
        FROM covid_climate
        ORDER BY year
    """)
    basic_stats.show()
    return basic_stats

def summary_analysis(spark):
    print("\n" + "="*50)
    print("Resumen")
    print("="*50 + "\n")
    summary_stats = spark.sql("""
        SELECT
            COUNT(*) as total_years,
            SUM(total_cases) as total_cases_all_years,
            ROUND(AVG(avg_temp), 2) as avg_temperature,
            ROUND(MIN(avg_temp), 2) as min_temperature,
            ROUND(MAX(avg_temp), 2) as max_temperature,
            ROUND(AVG(hospitalized_cases * 100.0 / total_cases), 2) as avg_hospitalization_rate
        FROM covid_climate
    """)
    summary_stats.show()
    return summary_stats

def correlation_analysis(spark):
    print("\n" + "="*50)
    print("Analisis de Correlacion")
    print("="*50 + "\n")
    correlation_stats = spark.sql("""
        SELECT
            ROUND(corr(avg_temp, total_cases), 4) as temp_cases_correlation,
            ROUND(corr(avg_temp, hospitalized_cases), 4) as temp_hospitalization_correlation,
            ROUND(corr(avg_temp, severe_cases), 4) as temp_severity_correlation
        FROM covid_climate
    """)
    correlation_stats.show()
    return correlation_stats

def temperature_category_analysis(spark):
    print("\n" + "="*50)
    print("Analisis de Temperatura por categoría")
    print("="*50 + "\n")
    temp_analysis = spark.sql("""
        SELECT
            CASE
                WHEN avg_temp < 8 THEN 'Frio (<8°C)'
                WHEN avg_temp >= 8 AND avg_temp < 16 THEN 'Moderado (8-16°C)'
                ELSE 'Calido (>16°C)'
            END as temperature_category,
            COUNT(*) as years_count,
            ROUND(AVG(total_cases), 0) as avg_cases,
            ROUND(AVG(hospitalized_cases), 0) as avg_hospitalizations,
            ROUND(AVG(hospitalized_cases * 100.0 / total_cases), 2) as avg_hospitalization_rate
        FROM covid_climate
        GROUP BY
            CASE
                WHEN avg_temp < 8 THEN 'Frio (<8°C)'
                WHEN avg_temp >= 8 AND avg_temp < 16 THEN 'Moderado (8-16°C)'
                ELSE 'Calido (>16°C)'
            END
        ORDER BY avg_cases DESC
    """)

    temp_analysis.show()
    return temp_analysis

def save_result(basic_stats, summary_stats, correlation_stats, temp_analysis):
    print("GUARDANDO RESULTADOS...")
    try:
        basic_stats.coalesce(1).write.mode("overwrite").parquet("s3://proyecto3bigdata/refined/descriptive_analysis/basic_stats_by_year/")
        print("Estadísticas básicas guardadas correctamente.")
        
        summary_stats.coalesce(1).write.mode("overwrite").parquet("s3://proyecto3bigdata/refined/descriptive_analysis/summary_statistics/")
        print("Estadísticas de resumen guardadas correctamente.")

        correlation_stats.coalesce(1).write.mode("overwrite").parquet("s3://proyecto3bigdata/refined/descriptive_analysis/correlations/")
        print("Estadísticas de correlación guardadas correctamente.")

        temp_analysis.coalesce(1).write.mode("overwrite").parquet("s3://proyecto3bigdata/refined/descriptive_analysis/temperature_analysis/")
        print("Temperatura por categoría guardada correctamente.")
        print("\n->RESULTADOS GUARDADOS CORRECTAMENTE<-")
    except Exception as e:
        print("Error al guardar los resultados: ", e)
        raise

def main():
    print("Iniciando análisis descriptivo...")
    print("="*50)
    spark = create_spark_session()
    try:
        df = load_data(spark)
        basic_stats = basic_statistics_analysis(spark, df)
        summary_stats = summary_analysis(spark)
        correlation_stats = correlation_analysis(spark)
        temp_analysis = temperature_category_analysis(spark)
        save_result(basic_stats, summary_stats, correlation_stats, temp_analysis)
        print("Análisis descriptivo completado con éxito.")
        print("\n Los archivos fueron creados en refined/descriptive_analysis/")
    except Exception as e:
        print("Error durante el análisis descriptivo: ", e)
        raise
    finally:
        spark.stop()
        print("Sesión de Spark cerrada.")

if __name__ == "__main__":
    main()