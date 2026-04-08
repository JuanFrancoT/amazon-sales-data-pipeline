from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, to_date, when, trim, regexp_replace
import os


def main():

    # ==============================
    # 1. SESIÓN DE SPARK
    # ==============================
    spark = SparkSession.builder \
        .appName("AmazonSalesETL") \
        .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("🚀 Spark iniciado")

    # ==============================
    # 2. LECTURA (RAW)
    # ==============================
    bucket = "payments-bucket-etl"
    path_raw = f"s3a://{bucket}/raw/aws_sales.csv"

    df = spark.read.csv(path_raw, header=True, inferSchema=True)

    print("📥 Datos leídos")

    # ==============================
    # 3. TRANSFORMACIÓN
    # ==============================

    df_clean = df \
        .drop("Unnamed: 22", "index") \
        .withColumn("Date", to_date(col("Date"), "M-d-yy")) \
        .withColumn("ship-city", upper(trim(col("ship-city")))) \
        .withColumn("Amount", col("Amount").cast("double")) \
        .filter(col("Amount").isNotNull()) \
        .withColumn("is_high_value", when(col("Amount") > 500, True).otherwise(False))

    print("Transformaciones aplicadas")

    # ==============================
    # 4. RENOMBRAR COLUMNAS (CLAVE)
    # ==============================

    df_clean = df_clean \
        .withColumnRenamed("Order ID", "ORDER_ID") \
        .withColumnRenamed("Date", "DATE") \
        .withColumnRenamed("Status", "STATUS") \
        .withColumnRenamed("Amount", "AMOUNT") \
        .withColumnRenamed("ship-city", "CITY") \
        .withColumnRenamed("is_high_value", "IS_HIGH_VALUE")

    print("🔤 Columnas renombradas correctamente")

    # ==============================
    # 5. SELECCIONAR COLUMNAS FINALES
    # ==============================

    df_clean = df_clean.select(
        "ORDER_ID",
        "DATE",
        "STATUS",
        "AMOUNT",
        "CITY",
        "IS_HIGH_VALUE"
    )

    print("📊 Columnas finales seleccionadas")

    # ==============================
    # 6. VALIDACIONES
    # ==============================

    if df_clean.count() == 0:
        raise Exception("❌ Dataset vacío")

    if df_clean.filter(col("AMOUNT") < 0).count() > 0:
        raise Exception("❌ Hay valores negativos en AMOUNT")

    print("🛡️ Validaciones OK")

    # ==============================
    # 7. EVITAR ARCHIVOS VACÍOS
    # ==============================

    df_clean = df_clean.coalesce(1)

    # ==============================
    # 8. ESCRITURA A S3 (PARQUET)
    # ==============================

    path_processed = f"s3a://{bucket}/processed/sales_data_parquet"

    df_clean.write \
        .partitionBy("DATE") \
        .mode("overwrite") \
        .parquet(path_processed)

    print(f"✅ Datos guardados en S3: {path_processed}")

    # ==============================
    # 9. (OPCIONAL) CARGA DIRECTA A SNOWFLAKE
    # ==============================

    sfOptions = {
        "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfRole": "ACCOUNTADMIN"
    }

    print("❄️ Enviando a Snowflake...")
    print("Filas a cargar:", df_clean.count())
    df_clean.show(5, False)
    
    df_clean.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "STAGING.STG_SALES_DATA") \
        .mode("overwrite") \
        .save()

    print("✅ Datos cargados en Snowflake")
    print("Datos cargados :)")
    
    spark.stop()


if __name__ == "__main__":
    main()

