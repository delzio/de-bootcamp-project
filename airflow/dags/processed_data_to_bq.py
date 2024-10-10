# Source libraries
import os
import sys
from datetime import datetime
# pip install apache-airflow
from airflow import DAG
from airflow.operators import BashOperator

# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")

# Get GCP input data
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("GCP_BQ_DATASET")

# Get file structure data
gcs_raw_path = "raw/"
gcs_sample_path = "processed/sample_context/"
#spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
model_path = f"gs://{BUCKET}/model/raman_model.pk1"
execution_time = datetime.utcnow().isoformat()

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
    
    process_data_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            gs://{BUCKET}/pyspark_code/processed_to_bq.py \
            -- \
                --gcs_bucket={BUCKET}
                --dataset={DATASET}
                --gcs_raw_path={gcs_raw_path}
                --gcs_sample_path={gcs_sample_path}
                --project_id={PROJECT_ID}
                --current_time={execution_time}
                --full_backfill={False}
    """
    
    t1 = BashOperator(
        task_id='processed_data_to_bq_spark',
        bash_command=process_data_job
    )

    predict_concentration_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            gs://{BUCKET}/pyspark_code/pls_prediction_to_bq.py \
            -- \
                --gcs_bucket={BUCKET}
                --dataset={DATASET}
                --gcs_raw_path={gcs_raw_path}
                --project_id={PROJECT_ID}
                --pls_model_path={model_path}
                --current_time={execution_time}
    """
    
    t2 = BashOperator(
        task_id='predict_recent_sample_concentrations',
        bash_command=predict_concentration_job
    )

    t1 >> t2