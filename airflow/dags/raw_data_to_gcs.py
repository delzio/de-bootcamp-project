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
# import user defined functions
# import user defined functions
HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(f"{HOME}/dags")
from dag_functions import processed_to_bq

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
gcs_input_path = "raw/"
gcs_output_path = "processed/sample_context/"
gcs_raman_path = "processed/raman_context/"
spark_jar_path = "/.project/lib/gcs-connector-hadoop3-2.2.5.jar,/.project/lib/spark-3.5-bigquery-0.37.0.jar"

# Functions: 
# Send raw data as series of parquet files to GCS
# NOTE: Can take several minutes depending on internet speed
def raw_to_gcs(gcs_bucket, gcs_path, local_data_file, local_data_path, temp_path):
    logging.info(f"{gcs_bucket}")
    chunk_size = 10000
    next_id = 1

    # Loop through csv and send chunks of data as parquet files to gcs
    for chunk in pd.read_csv(local_data_path+local_data_file, sep=",", chunksize=chunk_size):
        # add unique id to data records
        nrow = chunk.shape[0]
        ids = list(range(next_id,next_id+nrow))
        chunk["id"] = ids

        # convert df to parquet file
        parquet_file = f"{temp_path}insulin_batch_set_{ids[0]}.parquet"
        chunk.to_parquet(parquet_file, engine = 'pyarrow')

        # upload to gcs
        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket)
        bucket_path = os.path.join(gcs_path, os.path.basename(parquet_file))
        blob = bucket.blob(bucket_path)
        blob.upload_from_filename(parquet_file)
        logging.info(f"Chunk starting at row {ids[0]} sent to GCS bucket")

        # set next id
        next_id = ids[len(ids)-1] + 1

# Generate Batch and Sample Time context and send as series of parquet files to GCS
# NOTE: Can take several minutes depending on internet speed
def batch_context_to_gcs(gcs_bucket, gcs_input_path, gcs_output_path, credentials_location, spark_jar_location):
    # start spark standalone instance with worker
    start_spark_master = "cd $SPARK_HOME && ./sbin/start-master.sh --port 7077"
    start_spark_worker = "cd $SPARK_HOME && ./sbin/start-worker.sh spark://127.0.0.1:7077"
    start_master_process = subprocess.Popen(start_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_master_output, start_master_error = start_master_process.communicate()
    logging.info("spark master process created")
    start_worker_process = subprocess.Popen(start_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_worker_output, start_worker_error = start_worker_process.communicate()
    logging.info("spark worker process created")

    # define spark configuration
    conf = SparkConf() \
        .setMaster("spark://127.0.0.1:7077") \
        .setAppName("process_raw_data") \
        .set("spark.jars", spark_jar_location) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    logging.info("spark config created")

    # set up spark context
    sc = SparkContext(conf=conf)
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    logging.info("spark context created")

    # start spark session using standalone cluster
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    logging.info("spark session started")

    # pull data from GCS Bucket into spark df, selecting only necessary columns for context
    logging.info(f'pulling files from: gs://{gcs_bucket}/{gcs_input_path}*')
    df= spark.read.parquet(f'gs://{gcs_bucket}/{gcs_input_path}*') \
        .select(["id","Time (h)"])
    # sort by id, save row count
    df = df.orderBy("id")
    nrows = df.count()
    logging.info("data read from gcs")

    # Find new batch start indeces
    batch_start_df = df \
        .filter(df["Time (h)"] == 0.2) \
        .select("id") \
        .withColumnRenamed("id","batch_start_id") \
        .withColumn("Batch Number",F.monotonically_increasing_id()+1)
    # Add next batch id for join clause
    window_frame = Window.orderBy("batch_start_id")
    batch_start_df = batch_start_df.withColumn("next_batch_start_id", F.lead("batch_start_id").over(window_frame))
    # fill final next_batch_start_id with nrow df_sorted + 1
    batch_start_df = batch_start_df.fillna(nrows+1)
    # join batch number context to df
    df_processed = df.join(batch_start_df, (df.id >= batch_start_df.batch_start_id) & (df.id < batch_start_df.next_batch_start_id ), "inner") \
        .drop(*["batch_start_id","next_batch_start_id"])
    logging.info("df_processed created")

    # we want to simulate that 30 batches worth of the dataset have already been completed, while the final 70 are still to be peformed
    completed_batches = 30
    first_new_batch = df_processed \
        .filter(df_processed["Batch Number"] == completed_batches+1) \
        .select("id") \
        .head()[0]
    # generate artificial sample production timestamps at around 0.06s per sample (of course this is highly accelerated for quick demonstration purposes)
    # the final sample will be consumed a little over an hour from the current time
    ts_current = datetime.utcnow()
    ts_first_30_batches = [ts_current - i*timedelta(seconds=0.06) for i in range(1,first_new_batch)]
    ts_first_30_batches.reverse()
    ts_last_70_batches = [ts_current + i*timedelta(seconds=0.06) for i in range(first_new_batch,nrows+1)]
    sample_ts = ts_first_30_batches
    sample_ts.extend(ts_last_70_batches)
    # join sample ts to processed_df
    sample_ts_df = spark.createDataFrame([Row(index=i+1, sample_ts=sample_ts[i]) for i in range(nrows)])
    df_batch_context = df_processed.join(sample_ts_df, df_processed.id == sample_ts_df.index, "inner") \
        .drop("index")
    logging.info("df_batch_context created")
    
    # send batch context df to gcs bucket under processed folder with 4 partitions
    df_batch_context \
        .repartition(4) \
        .write.mode('overwrite').parquet(f'gs://{gcs_bucket}/{gcs_output_path}')

    logging.info("context data sent to GCS")
    
    # stop spark
    spark.stop()
    stop_spark_master = "cd $SPARK_HOME && ./sbin/stop-master.sh"
    stop_spark_worker = "cd $SPARK_HOME && ./sbin/stop-worker.sh"
    stop_master_process = subprocess.Popen(stop_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_master_output, stop_master_error = stop_master_process.communicate()
    stop_worker_process = subprocess.Popen(stop_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_worker_output, stop_worker_error = stop_worker_process.communicate()
    logging.info("Spark processes stopped")

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
            True
        ]
    )

    t1 >> t2 >> t3
    