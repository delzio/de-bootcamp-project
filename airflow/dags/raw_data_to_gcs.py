# Source libraries
import os
import sys
from datetime import datetime, timedelta
# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(f"{HOME}/dags")
from dag_functions import DagFunctions

# Get GCP input data
PROJECT_ID = os.environ.get("TF_VAR_project")
BUCKET = os.environ.get("TF_VAR_bucket")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("TF_VAR_dataset")
REGION = os.environ.get("TF_VAR_region")


# Get file structure data
local_data_path = "/.project/data/raw/Mendeley_data/"
temp_path = "/.project/data/raw/temp/"
local_data_file = "100_Batches_IndPenSim_V3.csv"
gcs_input_path = "raw/"
gcs_output_path = "processed/sample_context/"
#spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"
execution_time = datetime.utcnow().isoformat()

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
    
    wait_for_gcp_auth = PythonOperator(
        task_id='wait_for_gcp_auth',
        python_callable=DagFunctions.get_dag_status,
        op_args=[
            'gcp_service_account_auth_dag',
            'authenticate_service_account',
            1
        ],
        retries = 10, # unlimited retries until the service account auth dag is run successfully
        retry_delay=timedelta(seconds=20)
    )

    t1 = PythonOperator(
        task_id='raw_to_gcs_task',
        python_callable=DagFunctions.raw_to_gcs,
        op_args=[
            BUCKET,
            gcs_input_path,
            local_data_file,
            local_data_path,
            temp_path
        ]
    )

    batch_context_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            --region={REGION} \
            --py-files=gs://{BUCKET}/pyspark_code/dag_functions.py \
            --properties='spark.pyspark.virtualenv=/opt/venv' \
            gs://{BUCKET}/pyspark_code/batch_context_to_gcs.py \
            -- \
                --gcs_bucket={BUCKET} \
                --gcs_input_path={gcs_input_path} \
                --gcs_output_path={gcs_output_path}
    """
    
    t2 = BashOperator(
        task_id='add_batch_context_spark',
        bash_command=batch_context_job
    )

    backfill_30_batches_job = f"""
        gcloud dataproc jobs submit pyspark \
            --cluster=spark-cluster \
            --region={REGION} \
            --py-files=gs://{BUCKET}/pyspark_code/dag_functions.py \
            --properties='spark.pyspark.virtualenv=/opt/venv' \
            gs://{BUCKET}/pyspark_code/processed_to_bq.py \
            -- \
                --gcs_bucket={BUCKET} \
                --dataset={DATASET} \
                --gcs_raw_path={gcs_input_path} \
                --gcs_sample_path={gcs_output_path} \
                --project_id={PROJECT_ID} \
                --current_time={execution_time} \
                --full_backfill={True}
    """
    
    t3 = BashOperator(
        task_id='backfill_30_batches_spark',
        bash_command=backfill_30_batches_job
    )

    wait_for_gcp_auth >> t1 >> t2 >> t3
    