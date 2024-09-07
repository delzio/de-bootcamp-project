# Source libraries
import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# pip install google-cloud-storage
from google.cloud import storage
# pip install pyspark
import pyspark
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F
# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(f"{HOME}/dags")
from dag_functions import processed_to_bq, raw_to_gcs, batch_context_to_gcs

# Get GCP input data
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("GCP_BQ_DATASET")

# Get file structure data
local_data_path = "/.project/data/raw/Mendeley_data/"
temp_path = "/.project/data/raw/temp/"
local_data_file = "100_Batches_IndPenSim_V3.csv"
gcs_input_path = "raw/"
gcs_output_path = "processed/sample_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
execution_time = datetime.utcnow()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 12),
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="raw_data_ingestion_gcs_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    t1 = PythonOperator(
        task_id='raw_to_gcs_task',
        python_callable=raw_to_gcs,
        op_args=[
            BUCKET,
            gcs_input_path,
            local_data_file,
            local_data_path,
            temp_path
        ]
    )

    t2 = PythonOperator(
        task_id='batch_context_to_gcs_task',
        python_callable=batch_context_to_gcs,
        op_args=[
            BUCKET,
            gcs_input_path,
            gcs_output_path,
            CREDENTIALS,
            spark_jar_path
        ]
    )

    t3 = PythonOperator(
        task_id='backfill_initial_30_batches',
        python_callable=processed_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_input_path,
            gcs_output_path,
            PROJECT_ID,
            CREDENTIALS,
            spark_jar_path,
            execution_time,
            True
        ]
    )

    t1 >> t2 >> t3
    