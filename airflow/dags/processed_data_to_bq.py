# Source libraries
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Packages required for model
import joblib

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
gcs_raw_path = "raw/"
gcs_sample_path = "processed/sample_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
model_path = "/.project/data/processed/model/raman_pls_model.pk1"
pls_model = joblib.load(model_path)
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
    dag_id="processed_to_bq_dag",
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    wait_for_backfill = PythonOperator(
        task_id='wait_for_backfill',
        python_callable=dag_funcs.get_dag_status,
        op_args=[
            'raw_data_ingestion_gcs_dag',
            'backfill_30_batches_spark',
            24
        ],
        retries = 10, # unlimited retries until backfill completes
        retry_delay=timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id='process_recent_raman_results',
        python_callable=dag_funcs.processed_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_raw_path,
            gcs_sample_path,
            PROJECT_ID,
            execution_time
        ]
    )

    t2 = PythonOperator(
        task_id='predict_recent_sample_concentrations',
        python_callable=dag_funcs.pls_prediction_to_bq,
        op_args=[
            BUCKET,
            DATASET,
            gcs_raw_path,
            PROJECT_ID,
            pls_model,
            execution_time
        ]
    )

    wait_for_backfill >> t1 >> t2