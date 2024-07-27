# Source libraries
import os
import sys
from datetime import datetime
import pandas as pd
# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Packages required for model
import joblib

# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(f"{HOME}/dags")
from dag_functions import processed_to_bq, pls_prediction_to_bq

# Get GCP input data
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("GCP_BQ_DATASET")

# Get file structure data
gcs_raw_path = "raw/"
gcs_sample_path = "processed/sample_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
model_path = "/.project/data/processed/model/raman_pls_model.pk1"
pls_model = joblib.load(model_path)
execution_time = datetime.utcnow()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 12),
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="processed_to_bq_dag",
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    t1 = PythonOperator(
        task_id='processed_to_bq_task',
        python_callable=processed_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_raw_path,
            gcs_sample_path,
            PROJECT_ID,
            CREDENTIALS,
            spark_jar_path,
            execution_time
        ]
    )

    t2 = PythonOperator(
        task_id='pls_prediction_to_bq_task',
        python_callable=pls_prediction_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_raw_path,
            PROJECT_ID,
            CREDENTIALS,
            spark_jar_path,
            pls_model,
            execution_time
        ]
    )

    t1 >> t2