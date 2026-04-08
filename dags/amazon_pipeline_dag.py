from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# 1. Definimos los argumentos base
default_args = {
    'owner': 'JuanFranco',
    'retries': 0,
}

# 2. Usamos el Context Manager 'with' (Esto evita el error de start_date)
with DAG(
    'amazon_sales_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1), # Lo ponemos explícitamente aquí
    schedule_interval=None,
    catchup=False
) as dag:

    # ==============================
    # TASK 1: SPARK ETL
    # ==============================
    spark_task = BashOperator(
        task_id='run_spark_etl_job',
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        --master spark://spark:7077 \
        --packages net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
        --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
        /opt/spark_jobs/etl_job.py
        """
    )

    # ==============================
    # TASK 2: LOAD → STAGING (Lógica interna)
    # ==============================
    def load_to_staging_func():
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="STAGING"
        )
        try:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE STG_SALES_DATA")
            copy_query = """
            COPY INTO STG_SALES_DATA
            FROM (
                SELECT 
                    $1:ORDER_ID::STRING,
                    $1:DATE::DATE,
                    $1:STATUS::STRING,
                    $1:AMOUNT::FLOAT,
                    $1:CITY::STRING,
                    $1:IS_HIGH_VALUE::BOOLEAN
                FROM @AMAZON_DB.STAGING.S3_STAGE/processed/sales_data_parquet/
            )
            FILE_FORMAT = (TYPE = PARQUET);
            """
            cursor.execute(copy_query)
        finally:
            conn.close()

    load_stage_task = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_to_staging_func
    )

    # ==============================
    # TASK 3: STAGING → ANALYTICS
    # ==============================
    def transform_to_analytics_func():
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="ANALYTICS"
        )
        try:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE ANALYTICS.SALES_DATA")
            insert_query = "INSERT INTO ANALYTICS.SALES_DATA SELECT * FROM STAGING.STG_SALES_DATA"
            cursor.execute(insert_query)
        finally:
            conn.close()

    analytics_task = PythonOperator(
        task_id='transform_to_analytics',
        python_callable=transform_to_analytics_func
    )

    # ==============================
    # DEPENDENCIAS
    # ==============================
    spark_task >> load_stage_task >> analytics_task