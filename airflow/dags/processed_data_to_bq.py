# Source libraries
import os
from datetime import datetime, timedelta
# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# import user defined functions
from dag_functions import DagFunctions

# set airflow home
HOME = os.environ.get("AIRFLOW_HOME")

# Get GCP input data
PROJECT_ID = os.environ.get("TF_VAR_project")
BUCKET = os.environ.get("TF_VAR_bucket")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("TF_VAR_dataset")
REGION = os.environ.get("TF_VAR_region")

# Get file structure data
gcs_raw_path = "raw/"
gcs_sample_path = "processed/sample_context/"
#spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
model_path = "model/raman_model.pk1"
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

    wait_for_backfill = PythonOperator(
        task_id='wait_for_backfill',
        python_callable=DagFunctions.get_dag_status,
        op_args=[
            'raw_data_ingestion_gcs_dag',
            'backfill_30_batches_spark',
            24
        ],
        retries = 10, # unlimited retries until backfill completes
        retry_delay=timedelta(minutes=1)
    )

    process_data_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            --region={REGION} \
            --py-files=gs://{BUCKET}/pyspark_code/dag_functions.py \
            --properties='spark.pyspark.virtualenv=/opt/venv' \
            gs://{BUCKET}/pyspark_code/processed_to_bq.py \
            -- \
                --gcs_bucket={BUCKET} \
                --dataset={DATASET} \
                --gcs_raw_path={gcs_raw_path} \
                --gcs_sample_path={gcs_sample_path} \
                --project_id={PROJECT_ID} \
                --current_time={execution_time} \
                --full_backfill={False}
    """
    
    t1 = BashOperator(
        task_id='processed_data_to_bq_spark',
        bash_command=process_data_job
    )

    predict_concentration_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            --region={REGION} \
            --py-files=gs://{BUCKET}/pyspark_code/dag_functions.py \
            --properties='spark.pyspark.virtualenv=/opt/venv' \
            gs://{BUCKET}/pyspark_code/pls_prediction_to_bq.py \
            -- \
                --gcs_bucket={BUCKET} \
                --dataset={DATASET} \
                --gcs_raw_path={gcs_raw_path} \
                --project_id={PROJECT_ID} \
                --pls_model_path={model_path} \
                --current_time={execution_time}
    """
    
    t2 = BashOperator(
        task_id='predict_recent_sample_concentrations',
        bash_command=predict_concentration_job
    )

    wait_for_backfill >> t1 >> t2