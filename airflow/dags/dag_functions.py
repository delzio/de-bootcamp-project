#--- Function File for all functions used by raw_data_to_gcs dag ---
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

# Send Processed Data to BigQuery
def processed_to_bq(gcs_bucket, dataset, gcs_raw_path, gcs_sample_path, project_id, credentials_location, spark_jar_path, full_backfill=False):
    logging.info(f"{project_id},{gcs_bucket},{dataset}")
    # start spark standalone instance with worker
    start_spark_master = "cd $SPARK_HOME && ./sbin/start-master.sh --port 7078"
    start_spark_worker = "cd $SPARK_HOME && ./sbin/start-worker.sh spark://127.0.0.1:7078"
    start_master_process = subprocess.Popen(start_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_master_output, start_master_error = start_master_process.communicate()
    start_worker_process = subprocess.Popen(start_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_worker_output, start_worker_error = start_worker_process.communicate()
    logging.info("spark master + worker started")

    # define spark configuration
    conf = SparkConf() \
        .setMaster("spark://127.0.0.1:7078") \
        .setAppName("process_raw_data") \
        .set("spark.jars", spark_jar_path) \
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

    # Start Spark session using standalone cluster
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    logging.info("spark session created")

    # Gather Data
    df_raw_values = spark.read.parquet(f'gs://{gcs_bucket}/{gcs_raw_path}*.parquet')
    logging.info("raw data pulled from gcs")
    df_context = spark.read.parquet(f'gs://{gcs_bucket}/{gcs_sample_path}*.parquet')
    logging.info("sample context data pulled from gcs")
    # filter columns
    raman_cols = ["id"," 1-Raman spec recorded","2-PAT control(PAT_ref:PAT ref)","Fault flag"]
    sample_cols = ["id","Time (h)","Penicillin concentration(P:g/L)","Fault reference(Fault_ref:Fault ref)","0 - Recipe driven 1 - Operator controlled(Control_ref:Control ref)"]
    # split raw df into relevant sample values and raman measurement data
    df_samples = df_raw_values.select(sample_cols) \
        .withColumnRenamed("id","id_sample")
    df_raman = df_raw_values.select(raman_cols) \
        .withColumnRenamed("id","id_raman")
    logging.info("raman and sample spark dfs created")

    # find most recent existing record in T_SAMPLE_CONTEXT
    # gather all sample records produced in the last 5 minutes
    current_time = datetime.utcnow()
    if full_backfill is True:
        back_time = current_time - timedelta(days=2)
    else:
        back_time = current_time - timedelta(minutes=7)
    current_ts = current_time.strftime("%Y-%m-%d %H:%M:%S")
    back_ts = back_time.strftime("%Y-%m-%d %H:%M:%S")
    date_range = (current_ts,back_ts)
    where_clause = """sample_ts BETWEEN TO_TIMESTAMP('{1}','yyyy-MM-dd HH:mm:ss') AND TO_TIMESTAMP('{0}','yyyy-MM-dd HH:mm:ss')""".format(*date_range)
    try:
        most_recent_time = spark.read.format("bigquery") \
            .option("project",project_id) \
            .option("dataset",dataset) \
            .option("table","t_sample_context") \
            .load() \
            .where(where_clause) \
            .agg(F.max("sample_ts")) \
            .collect()[0][0]
        if (most_recent_time is None):
            most_recent_time = back_time
            logging.info(f"No existing records found within 7 minutes of current time")
        else:
            logging.info(f"Some existing records found - starting after most recent existing time: {most_recent_time}: {where_clause}")
    except Exception as e:
        logging.info(f"No existing records found within 7 minutes of current time")
        most_recent_time = back_time
    most_recent_ts = most_recent_time.strftime("%Y-%m-%d %H:%M:%S")

    # gather new data that has not been traced into T_SAMPLE_CONTEXT
    date_range = (current_ts,most_recent_ts)
    df_context = df_context.select(["id","Batch Number","sample_ts"]) \
        .where(where_clause) \
        .withColumnRenamed("Batch Number","batch_number")
    # join to raman and sample dfs
    df_sample_context = df_samples.join(df_context,df_samples.id_sample == df_context.id,"inner").drop("id_sample")
    df_raman_context = df_raman.join(df_context,df_raman.id_raman == df_context.id,"inner").drop("id_raman")
    logging.info(f"Raman and sample value data joined to context data")

    # rename columns
    sample_colnames = ["time_hrs","penicillin_concentration_g_l","fault_reference","recipe_0_or_operator_1_controlled","id","batch_number","sample_ts"]
    raman_colnames = ["1_raman_spec_recorded","2_pat_control","fault_flag","id","batch_number","sample_ts"]
    df_sample_context = df_sample_context.toDF(*sample_colnames)
    df_raman_context = df_raman_context.toDF(*raman_colnames)
    # fill null values with 0
    df_sample_context = df_sample_context.fillna(0)
    df_raman_context = df_raman_context.fillna(0)

    # define schema for T_SAMPLE_CONTEXT
    sample_schema = T.StructType([
        T.StructField("time_hrs",T.DoubleType()),
        T.StructField("penicillin_concentration_g_l",T.DoubleType()),
        T.StructField("fault_reference",T.LongType()),
        T.StructField("recipe_0_or_operator_1_controlled",T.LongType()),
        T.StructField("id",T.IntegerType()),
        T.StructField("batch_number",T.LongType()),
        T.StructField("sample_ts",T.TimestampType())
    ])

    # define schema for T_RAMAN_CONTEXT
    raman_schema = T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("batch_number",T.LongType()),
        T.StructField("sample_ts",T.TimestampType()),
        T.StructField("1_raman_spec_recorded",T.LongType()),
        T.StructField("2_pat_control",T.LongType()),
        T.StructField("fault_flag",T.LongType())
    ])

    # add new sample context data to table
    # we want to partition by timestamp to make querying by timestamp ranges more efficient
    # we want to cluster by batch_number to group data from the same batches
    logging.info(f"About to insert new data to T_SAMPLE_CONTEXT")
    df_sample_context.write.format("bigquery") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .option("table", f"{project_id}.{dataset}.t_sample_context") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .option("schema", sample_schema.json()) \
        .option("partitionField","sample_ts") \
        .option("clusteredFields","batch_number") \
        .mode("append") \
        .save()

    # add new raman context data to table
    # we want to partition by timestamp to make querying by timestamp ranges more efficient
    logging.info(f"About to insert new data to T_RAMAN_CONTEXT")
    df_raman_context.write.format("bigquery") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .option("table", f"{project_id}.{dataset}.t_raman_context") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .option("schema", raman_schema.json()) \
        .option("partitionField","sample_ts") \
        .mode("append") \
        .save()
    
    # stop spark
    spark.stop()
    stop_spark_master = "cd $SPARK_HOME && ./sbin/stop-master.sh"
    stop_spark_worker = "cd $SPARK_HOME && ./sbin/stop-worker.sh"
    stop_master_process = subprocess.Popen(stop_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_master_output, stop_master_error = stop_master_process.communicate()
    stop_worker_process = subprocess.Popen(stop_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_worker_output, stop_worker_error = stop_worker_process.communicate()
    logging.info("Spark processes stopped")
    

# Predict Penicillin Concentration Using PLS Model and send to T_RAMAN_PREDICTION
def pls_prediction_to_bq(gcs_bucket, dataset, gcs_raw_path, project_id, credentials_location, spark_jar_path, pls_model):
    # start spark standalone instance with worker
    start_spark_master = "cd $SPARK_HOME && ./sbin/start-master.sh --port 7079"
    start_spark_worker = "cd $SPARK_HOME && ./sbin/start-worker.sh spark://127.0.0.1:7079"
    start_master_process = subprocess.Popen(start_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_master_output, start_master_error = start_master_process.communicate()
    start_worker_process = subprocess.Popen(start_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    start_worker_output, start_worker_error = start_worker_process.communicate()
    logging.info("spark master + worker started")

    # define spark configuration
    conf = SparkConf() \
        .setMaster("spark://127.0.0.1:7079") \
        .setAppName("predict_concentration_data") \
        .set("spark.jars", spark_jar_path) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.executor.memory","4g") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    logging.info("spark conf created")

    # set up spark context
    sc = SparkContext(conf=conf)
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true") 
    logging.info("spark context created")

    # Start Spark session using standalone cluster
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    logging.info("spark session created")
    
    # find most recent existing record in T_RAMAN_PREDICTION
    # gather all raman records produced in the last 7 minutes
    current_time = datetime.utcnow()
    back_time = current_time - timedelta(minutes=7)
    current_ts = current_time.strftime("%Y-%m-%d %H:%M:%S")
    back_ts = back_time.strftime("%Y-%m-%d %H:%M:%S")
    date_range = (current_ts,back_ts)
    where_clause = """sample_ts BETWEEN TO_TIMESTAMP('{1}','yyyy-MM-dd HH:mm:ss') AND TO_TIMESTAMP('{0}','yyyy-MM-dd HH:mm:ss')""".format(*date_range)
    try:
        most_recent_prediction = spark.read.format("bigquery") \
                .option("project",project_id) \
                .option("dataset",dataset) \
                .option("table","t_raman_prediction") \
                .load() \
                .where(where_clause) \
                .agg(F.max("sample_ts")) \
                .collect()[0][0]
        if (most_recent_prediction is None):
            most_recent_prediction = back_time
            logging.info(f"No existing records found within 7 minutes of current time")
        else:
            logging.info(f"Some existing records found - starting after most recent existing time: {most_recent_prediction}: {where_clause}")
    except Exception as e:
        logging.info(f"No existing records found within 7 minutes of current time")
        most_recent_prediction = back_time
    most_recent_ts = most_recent_prediction.strftime("%Y-%m-%d %H:%M:%S")

    # Gather New context data that has not been traced into T_RAMAN_PREDICTION
    date_range = (current_ts,most_recent_ts)
    where_clause = """sample_ts BETWEEN TO_TIMESTAMP('{1}','yyyy-MM-dd HH:mm:ss') AND TO_TIMESTAMP('{0}','yyyy-MM-dd HH:mm:ss')""".format(*date_range)
    raman_context_ids = spark.read.format("bigquery") \
            .option("project",project_id) \
            .option("dataset",dataset) \
            .option("table","t_sample_context") \
            .load() \
            .select(["id","penicillin_concentration_g_l"]) \
            .where(where_clause)
    logging.info(f"Found sample context data")
    
    # Join to Raman Spectra Data
    # Gather raw data
    df_raw_values = spark.read.parquet(f'gs://{gcs_bucket}/{gcs_raw_path}*.parquet')
    logging.info(f"Found raw raman measurement data")
    # filter columns
    raman_cols = ["id"]
    raman_cols.extend([str(i) for i in list(range(689,2089)[::-1])])
    # separate raw df into relevant raman measurement data
    df_raman = df_raw_values.select(raman_cols) \
        .withColumnRenamed("id","id_raman")
    # join to raman_context_ids to filter out old data
    df_raman_new = df_raman.join(raman_context_ids, df_raman.id_raman == raman_context_ids.id, "inner").drop("id_raman")
    logging.info(f"Joined raman measurement data to sample context")
    # Convert to Pandas for modeling
    df_raman_predict = df_raman_new.toPandas()
    logging.info(f"Created modeling df in pandas")

    # Calculate Derivative
    df_raman_spectra = df_raman_predict.copy()
    df_raman_test = df_raman_spectra
    df_raman_test.drop("id", axis=1, inplace=True)
    df_raman_test.drop("penicillin_concentration_g_l", axis=1, inplace=True)
    rcbw = RangeCut(0, df_raman_test.shape[1])
    data = rcbw.fit_transform(df_raman_test)
    raman_df = pd.DataFrame(data)
    lc = LinearCorrection()
    spectra_baseline = lc.fit_transform(raman_df)
    sgf = SavitzkyGolayFilter(window_size=15, polynomial_order=2)
    spectra_norm = sgf.fit_transform(spectra_baseline)
    nw = NorrisWilliams(window_size=15, gap_size=3, derivative_order=1)
    spectra_derivative = pd.DataFrame(nw.fit_transform(spectra_norm))
    logging.info(f"Calculated spectra derivative")

    # Create PLS Model
    x = spectra_derivative
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    predicted_concentration = pls_model.predict(x_scaled)
    logging.info(f"Calculated penicillin concentration predictions")

    # Join to sample measurement data
    df_raman_compare = pd.concat([df_raman_predict[["id","penicillin_concentration_g_l"]], pd.DataFrame({'predicted_penicillin_concentration': predicted_concentration})], axis=1)

    # Convert back to spark df
    df_raman_feature = spark.createDataFrame(df_raman_compare)

    # define schema for T_RAMAN_PREDICT
    predict_schema = T.StructType([
        T.StructField("id",T.IntegerType()),
        T.StructField("penicillin_concentration_g_l",T.DoubleType()),
        T.StructField("predicted_penicillin_concentration",T.DoubleType())
    ])

    # add new sample context data to T_RAMAN_PREDICTION
    logging.info(f"About to insert new data to T_RAMAN_PREDICTION")
    df_raman_feature.write.format("bigquery") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .option("table", f"{project_id}.{dataset}.t_raman_prediction") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .option("schema", predict_schema.json()) \
        .mode("append") \
        .save()
    logging.info(f"Prediction results sent to T_RAMAN_PREDICTION")
    
    # stop spark
    spark.stop()
    stop_spark_master = "cd $SPARK_HOME && ./sbin/stop-master.sh"
    stop_spark_worker = "cd $SPARK_HOME && ./sbin/stop-worker.sh"
    stop_master_process = subprocess.Popen(stop_spark_master, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_master_output, stop_master_error = stop_master_process.communicate()
    stop_worker_process = subprocess.Popen(stop_spark_worker, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop_worker_output, stop_worker_error = stop_worker_process.communicate()
    logging.info("Spark processes stopped")