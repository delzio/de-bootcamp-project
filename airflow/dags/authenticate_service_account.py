# Source libraries
import os
from datetime import datetime
# pip install apache-airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")

# Get GCP input data
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 12),
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcp_service_account_auth_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    gcp_service_auth_job = f"gcloud auth activate-service-account --key-file={CREDENTIALS}"
    
    t1 = BashOperator(
        task_id='authenticate_service_account',
        bash_command=gcp_service_auth_job
    )

    t1
    