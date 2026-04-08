FROM apache/airflow:2.7.0
USER root
RUN apt-get update && apt-get install -y docker.io && apt-get clean
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon==8.3.0 boto3 python-dotenv
RUN pip install python-dotenv