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
from dag_functions import DagFunctions

# Get GCP input data
PROJECT_ID = os.environ.get("TF_VAR_project")
BUCKET = os.environ.get("TF_VAR_bucket")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("TF_VAR_dataset")

# Get file structure data
local_data_path = "/.project/data/raw/Mendeley_data/"
temp_path = "/.project/data/raw/temp/"
local_data_file = "100_Batches_IndPenSim_V3.csv"
gcs_input_path = "raw/"
gcs_output_path = "processed/sample_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
execution_time = datetime.utcnow()

# Initialize DagFunctions
dag_funcs = DagFunctions(credentials_location=CREDENTIALS, spark_jar_path=spark_jar_path)

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
        python_callable=dag_funcs.raw_to_gcs,
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
        python_callable=dag_funcs.batch_context_to_gcs,
        op_args=[
            BUCKET,
            gcs_input_path,
            gcs_output_path
        ]
    )

    t3 = PythonOperator(
        task_id='backfill_initial_30_batches',
        python_callable=dag_funcs.processed_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_input_path,
            gcs_output_path,
            PROJECT_ID,
            execution_time,
            True
        ]
    )

    t1 >> t2 >> t3
    