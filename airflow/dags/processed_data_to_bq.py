# Source libraries
import os
import sys
import logging
import subprocess
from datetime import datetime, timedelta
import pandas as pd
# pip install apache-airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# pip install google-cloud-storage
from google.cloud import storage
# pip install apache-airflow-providers-google
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
#import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
# pip install pyspark
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
# Packages required for modeling
import joblib
from chemotools.baseline import LinearCorrection
from chemotools.smooth import SavitzkyGolayFilter
from chemotools.derivative import NorrisWilliams
from chemotools.feature_selection import RangeCut
from sklearn.preprocessing import StandardScaler
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
local_data_path = "/.project/data/raw/Mendeley_data/"
temp_path = "/.project/data/raw/temp/"
local_data_file = "100_Batches_IndPenSim_V3.csv"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
gcs_raw_path = "raw/"
gcs_sample_path = "processed/sample_context/"
gcs_raman_path = "processed/raman_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
model_path = "/.project/data/processed/model/raman_pls_model.pk1"
pls_model = joblib.load(model_path)

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
            spark_jar_path
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
            pls_model
        ]
    )

    t1 >> t2