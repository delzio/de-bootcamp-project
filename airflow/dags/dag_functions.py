#--- Function File for all functions used by raw_data_to_gcs dag ---
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import pytz
from tempfile import TemporaryFile
# pip install google-cloud-storage
from google.cloud import storage
# pip install pyspark
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
# Packages required for modeling
from chemotools.baseline import LinearCorrection
from chemotools.smooth import SavitzkyGolayFilter
from chemotools.derivative import NorrisWilliams
from chemotools.feature_selection import RangeCut
from sklearn.preprocessing import StandardScaler
import joblib

class DagFunctions:
    '''
    ETL functions used by airflow DAGs to populate GCP buckets and BigQuery datasets with Raman Modeling results

    :start_spark_session: initializes a spark session
    :get_dag_status: gets the most recent run status of the specified dag task using the airflow web api
    :raw_to_gcs: sends raw data extract to GCS bucket
    :batch_context_to_gcs: adds batch number and sample time context data to GCS bucket
    :processed_to_bq: traces the raw sample and raman data as they are "created" and sends processed data to the BigQuery dataset
    :pls_prediction_to_bq: predicts penicillin concentration by applying the PLS model to the processed data and sends results to BigQuery table T_RAMAN_PREDICTION
    '''


    @staticmethod
    def start_spark_session():
        '''Start spark session from GCP Dataproc cluster instance'''
        spark = SparkSession.builder \
            .appName("de_bootcamp_insulin_project") \
            .getOrCreate()
        
        return spark


    @staticmethod
    def get_dag_status(dag_id: str, task_id: str, last_n_hours: int):
        '''
        Gets the status of the most recent run of the specified dag task and returns error if status != "Success"
        :param dag_id: airflow dag id containing task of interest
        :param task_id: airflow task id to get status of
        :param last_n_hours: integer defining lookback time in hours to search for task status within
        '''

        # Airflow web server URL and endpoint
        airflow_url = 'http://airflow-webserver:8080/api/v1'

        # Get start time to search for dag execution
        time_window = timedelta(hours=last_n_hours)  
        execution_time = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_time = execution_time - time_window
        start_time_str = start_time.isoformat()

        # Get the last task instance details
        response = requests.get(f'{airflow_url}/dags/{dag_id}/dagRuns',params={'start_date_gte': start_time_str},auth=HTTPBasicAuth('airflow', 'airflow'))

        # Parse the JSON response
        dag_instances = response.json()

        # Check the latest task instance status
        if dag_instances['dag_runs']:
            dag_instances['dag_runs'].sort(key=lambda x: x['execution_date'], reverse=True)
            last_instance = dag_instances['dag_runs'][0]
            logging.info(f"Last run at {last_instance['execution_date']}")
            if last_instance['state'] != 'success':
                raise ValueError(f"Most recent execution of {task_id} resulted in status = '{last_instance['state']}'")
        else:
            raise ValueError(f"No prior execution of {task_id}")


    @staticmethod
    def raw_to_gcs(gcs_bucket: str, gcs_path: str, local_data_file: str, local_data_path: str, temp_path: str):
        '''
        Moves full raw dataset to GCS as series of parquet files
        NOTE: Can take several minutes depending on internet speed
        :param gcs_bucket: name of GCS bucket
        :param gcs_path: GCS bucket storage path for data output
        :param local_data_file: name of source data file on local machine
        :param local_data_path: local path to source data file
        :param temp_path: local temporary path to save temporary files to
        '''

        logging.info(f"{gcs_bucket}")
        chunk_size = 10000
        next_id = 1

        # CSV file data is large. Send chunks of data as parquet files to gcs
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
            next_id = ids[-1] + 1


    @staticmethod
    def batch_context_to_gcs(gcs_bucket: str, gcs_input_path: str, gcs_output_path: str):
        '''
        Generates batch number and sample time contextualizing data from raw data and sends as series of parquet files to GCS using Spark
        NOTE: Can take several minutes depending on internet speed
        :param gcs_bucket: name of GCS bucket
        :param gcs_input_path: GCS bucket storage path of raw data input
        :param gcs_output_path: GCS bucket storage path of contextualizing data output
        '''
        # Start spark session
        spark = DagFunctions.start_spark_session()
        logging.info("spark session started")

        # pull raw data from GCS Bucket into spark df, selecting only necessary columns for context
        logging.info(f'pulling files from: gs://{gcs_bucket}/{gcs_input_path}*.parquet')
        df= spark.read.parquet(f'gs://{gcs_bucket}/{gcs_input_path}*.parquet') \
            .select(["id","Time (h)"])
        # sort by id, save row count
        df = df.orderBy("id")
        nrows = df.count()
        logging.info("data read from gcs")

        # Find new batch start indeces and add batch number identifier
        batch_start_df = df \
            .filter(df["Time (h)"] == 0.2) \
            .select("id") \
            .withColumnRenamed("id","batch_start_id") \
            .withColumn("Batch Number",F.monotonically_increasing_id()+1)
        # Add next batch id for join clause
        window_frame = Window.orderBy("batch_start_id")
        batch_start_df = batch_start_df.withColumn("next_batch_start_id", F.lead("batch_start_id").over(window_frame))
        # final next_batch_start_id does not exist so need to fill value above final record id
        batch_start_df = batch_start_df.fillna(nrows+1)
        # join batch number context to raw data df
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
        ts_last_70_batches = [ts_current + i*timedelta(seconds=0.06) for i in range(nrows-first_new_batch+1)]
        sample_ts = ts_first_30_batches
        sample_ts.extend(ts_last_70_batches)
        # join sample ts to processed_df
        sample_ts_df = spark.createDataFrame([Row(index=i+1, sample_ts=sample_ts[i]) for i in range(nrows)])
        df_batch_context = df_processed.join(sample_ts_df, df_processed.id == sample_ts_df.index, "inner") \
            .drop("index")
        logging.info("df_batch_context created")
        
        # send batch context df to gcs bucket under processed folder with 4 partitions
        logging.info(f'sending context data to gs://{gcs_bucket}/{gcs_output_path}')
        df_batch_context \
            .repartition(4) \
            .write.mode('overwrite').parquet(f'gs://{gcs_bucket}/{gcs_output_path}')

        logging.info("context data sent to GCS")
        
        # stop spark session
        spark.stop()
        logging.info("Spark processes stopped")
    

    @staticmethod
    def processed_to_bq(gcs_bucket: str, dataset: str, gcs_raw_path: str, gcs_sample_path: str, project_id: str, current_time: datetime, full_backfill: bool = False):
        '''
        Simulates tracing the raw sample and raman data as they are "created" (based on sample_ts) into processed data tables in BigQuery using Spark
        :param gcs_bucket: name of GCS bucket
        :param dataset: name of BigQuery dataset
        :param gcs_raw_path: GCS bucket storage path of raw data input
        :param gcs_sample_path: GCS bucket storage path of sample data input
        :param project_id: GCP project id
        :param current_time: datetime instance of the current timestamp dag is run at
        :full_backfill: backfills all 30 batches "created" prior to dag run if true, backfills batches "created" within last 7 minutes of current_time if false
        '''
        # Start spark session
        spark = DagFunctions.start_spark_session()
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
        # if full_backfill is False: gather all sample records produced in the last 7 minutes (dag will execute every 5, want some overlap)
        # if full_backfill is True: gather all sample records produced in the last 2 days (this should be executed once at initial deployment to backfill the first 30 batches)
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
        where_clause = """sample_ts BETWEEN TO_TIMESTAMP('{1}','yyyy-MM-dd HH:mm:ss') AND TO_TIMESTAMP('{0}','yyyy-MM-dd HH:mm:ss')""".format(*date_range)
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
        
        # stop spark session
        spark.stop()
        logging.info("Spark processes stopped")
    

    @staticmethod
    def pls_prediction_to_bq(gcs_bucket: str, dataset: str, gcs_raw_path: str, project_id: str, pls_model_path: str, current_time: datetime):
        '''
        Predicts penicillin concentration using the trained PLS model and sends results to BigQuery table T_RAMAN_PREDICTION using Spark
        :param gcs_bucket: name of GCS bucket
        :param dataset: name of BigQuery dataset
        :param gcs_raw_path: GCS bucket storage path of raw data input
        :param project_id: GCP project id
        :param pls_model_path: GCS bucket storage path of PLS model file
        :param current_time: datetime instance of the current timestamp dag is run at
        '''
        # Start spark session
        spark = DagFunctions.start_spark_session()
        logging.info("spark session created")
        
        # find most recent existing record in T_RAMAN_PREDICTION

        # gather all raman records produced in the last 20 minutes
        back_time = current_time - timedelta(minutes=5)
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
                logging.info(f"No existing records found within 5 minutes of execution time")
            else:
                logging.info(f"Some existing records found - starting after most recent existing time: {most_recent_prediction}: {where_clause}")
        except Exception as e:
            logging.info(f"No existing records found within 5 minutes of execution time")
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

        if raman_context_ids.count() == 0:
            spark.stop()
            logging.info("No new sample context result from the last 5 minutes. No values added to T_RAMAN_PREDICTION.")
            return
        
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
        # After filtering down data using Spark, need to convert to Pandas for modeling
        df_raman_predict = df_raman_new.toPandas()
        logging.info(f"Created modeling df in pandas")

        # Perform data pre-processing
        df_raman_preprocessed = df_raman_predict[[col for col in df_raman_predict.columns if col not in ["id","penicillin_concentration_g_l"]]]
        lc = LinearCorrection()
        df_raman_preprocessed = lc.fit_transform(df_raman_preprocessed)
        sgf = SavitzkyGolayFilter(window_size=15, polynomial_order=2)
        df_raman_preprocessed = sgf.fit_transform(df_raman_preprocessed)
        nw = NorrisWilliams(window_size=15, gap_size=3, derivative_order=1)
        df_raman_preprocessed = pd.DataFrame(nw.fit_transform(df_raman_preprocessed))
        logging.info(f"Calculated spectra derivative")

        # Apply model to predict penicillin concentration
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(gcs_bucket)
        blob = bucket.blob(pls_model_path)
        with TemporaryFile() as temp_file:
            blob.download_to_file(temp_file)
            temp_file.seek(0)
            pls_model = joblib.load(temp_file)
        scaler = StandardScaler()
        df_raman_preprocessed = scaler.fit_transform(df_raman_preprocessed)
        predicted_concentration = pls_model.predict(df_raman_preprocessed)
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
        
        # stop spark session
        spark.stop()
        logging.info("Spark processes stopped")
