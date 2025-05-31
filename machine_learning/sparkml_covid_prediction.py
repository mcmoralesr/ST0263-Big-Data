from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("COVID-CLIMATE ML Prediction") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def load_data(spark):
    try:
        print("="*60)
        print("Cargando datos desde S3...")
        print("="*60)
        df = spark.read.parquet("s3a://proyecto3bigdata/trusted/joined_weather_covid/")
        print("Datos cargados correctamente.")
        df.show()
        return df
    except Exception as e:
        print("Error al cargar los datos:", e)
        raise

def prepare_features(df):
    print("="*60)
    print("Preparando las características para el modelo...")
    print("="*60)
    ml_data = df.select(col("year"),
                        col("avg_temp").alias("temperature"),
                        col("total_cases").alias("target_total_cases"),
                        col("hospitalized_cases").alias("target_hospitalized"),
                        col("severe_cases").alias("target_severe"),
                        col("icu_cases").alias("target_icu"))
    print("Datos preparados:")
    ml_data.show()
    return ml_data

def create_ml_models(spark, df):
    print("="*60)
    print("Creando modelos de Machine Learning...")
    print("="*60)

    feature_assembler = VectorAssembler(
        inputCols=["temperature"],
        outputCol="features"
    )
    results = []
    targets = [
        ("target_total_cases", "total_cases_model"),
        ("target_hospitalized", "hospitalized_model"),
        ("target_severe", "severe_model"),
        ("target_icu", "icu_model")
    ]

    for target_col, model_name in targets:
        print(f"\n Entrando al modelo: {model_name}")
        print("="*60)
        lr = LinearRegression(
            featuresCol = "features",
            labelCol = target_col,
            predictionCol = f"predicted_{target_col}"
        )
        pipeline = Pipeline(stages=[feature_assembler, lr])
        model = pipeline.fit(df)
        predictions = model.transform(df)
        evaluator = RegressionEvaluator(
            labelCol = target_col,
            predictionCol = f"predicted_{target_col}",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        r2_evaluator = RegressionEvaluator(
            labelCol= target_col,
            predictionCol= f"predicted_{target_col}",
            metricName="r2"
        )
        r2 = r2_evaluator.evaluate(predictions)
        print(f"📌 RMSE: {rmse:.2f}")
        print(f"📌 R2: {r2:.4f}")
        print("📌PREDICCIONES VS REALES")

        predictions.select(
            "year",
            "temperature",
            target_col,
            f"predicted_{target_col}"
        ).show()
        result_data = predictions.select(
            "year",
            "temperature",
            target_col,
            f"predicted_{target_col}",
            spark_round(col(f"predicted_{target_col}"), 0).alias(f"predicted_{target_col}_rounded")
        ).withColumn("model_name", lit(model_name)) \
            .withColumn("rmse", lit(rmse)) \
            .withColumn("r2", lit(r2))
        results.append((model_name, result_data, rmse, r2))

    return results

def create_future_predictions(spark, models_results):
    print("="*60)
    print("\nPREDICCIONES PARA ESCENARIOS FUTUROS")
    print("="*60)
    temp_scenarios = [
        (6.0, "Muy frio"),
        (8.0, "Frio"),
        (10.0, "Moderado"),
        (12.0, "Templado"),
        (14.0, "Calido")
    ]
    scenario_data = []
    for temp, description in temp_scenarios:
        scenario_data.append((temp, description))
    scenarios_df = spark.createDataFrame(scenario_data, ["temperature", "scenario_name"])
    print("📌 ESCENARIOS DE TEMPERATURA:")
    scenarios_df.show()
    return scenarios_df

def save_ml_results(spark, results, scenarios_df):
    print("="*60)
    print("GUARDANDO RESULTADOS DE MACHINE LEARNING...")
    print("="*60)
    try:
        for model_name, predictions, rmse, r2 in results:
            path = f"s3a://proyecto3bigdata/refined/ml_analysis/{model_name}_predictions/"
            predictions.coalesce(1).write.mode("overwrite").parquet(path)
            print(f"📌Resultados del modelo {model_name} guardados en {path}")
        scenarios_path = "s3a://proyecto3bigdata/refined/ml_analysis/temperature_scenarios/"
        scenarios_df.coalesce(1).write.mode("overwrite").parquet(scenarios_path)
        print(f"📌Escenarios guardados en {scenarios_path}")
        model_summary = []
        for model_name, _, rmse, r2 in results:
            model_summary.append((model_name, rmse, r2))
        summary_df = spark.createDataFrame(model_summary, ["model_name", "rmse", "r2"])
        summary_path = "s3a://proyecto3bigdata/refined/ml_analysis/model_summary/"
        summary_df.coalesce(1).write.mode("overwrite").parquet(summary_path)
        print(f"📌 Resumen de modelos guardado en: {summary_path}")
        print("\n📌 TODOS LOS RESULTADOS ML GUARDADOS CORRECTAMENTE")
    except Exception as e:
        print("Error al guardar los resultados de Machine Learning:", e)
        raise

def main():
    print("📌Iniciando análisis de Machine Learning...")
    print("🤖PI PU PI PU - MAQUINA PENSANDO... PI PU PI PU🤖 -> COVID-19 VS CLIMA MODELOS PREDICTIVOS PI PU PI PU")
    print("="*60)
    spark = create_spark_session()
    try:
        df = load_data(spark)
        ml_data = prepare_features(df)
        results = create_ml_models(spark, ml_data)
        scenarios_df = create_future_predictions(spark, results)
        save_ml_results(spark, results, scenarios_df)
        print("📌ANÁLISIS DE MACHINE LEARNING COMPLETADO CON ÉXITO")
    except Exception as e:
        print("Error en el análisis de Machine Learning:", e)
        raise
    finally:
        spark.stop()
        print("📌Sesión de Spark cerrada.")

if __name__ == "__main__":
    main()

