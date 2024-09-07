# Data Engineering Zoomcamp Final Project: ETL Pipeline for Insulin Production Data Analysis

## Introduction
This project uses the techniques learned throughout participation in the DataTalks.Club 2024 data engineering zoomcamp course to extract, transform, and load data from the large-scale manufacturing of Insulin into a dashboard for analysis. 

Project Evaluation Criteria Notes:

Problem Description can be found throughout this README in the [Background](#background), [Project Description](#project-description), [ETL Process](#etl-process), and [Database Schema](#database-schema) sections

Google Cloud Platform was used to develop this project

Data Ingestion was performed using batch pipelines consististing of two dags which run in parallel each with multiple tasks uploading to GCS buckets and BigQuery tables (view [ETL Process](#etl-process))

Biquery was used for data warehousing processed data and table partitioning and clustering was applied to multiple tables (view [Database Schema](#database-schema))

Complex data transformations were performed using Spark

The final data analysis dashboard is can be found here: [https://lookerstudio.google.com/u/0/reporting/ce52f512-fd15-4ed5-8637-fc065f89c0c8/page/fRIxD](https://lookerstudio.google.com/u/0/reporting/ce52f512-fd15-4ed5-8637-fc065f89c0c8/page/fRIxD)

This project should be fully reproduceable through docker using the [Installation and Usage Instructions](#installation-and-usage-instructions) (it is recommended to follow these instructions using a VM hosted by GCP)

## Background
Insulin is a vital protein product used in the treatment of diabetes. It is produced by growing living cells which have been genetically engineered to produce the Insulin protein and is later purified from these cells and all other impurities. To understand the amount of Insulin produced, samples are typically taken at several intervals throughout the manufacturing process. This sampling is invasive, leading to delays in manufacturing and increased risk of contaminations. Raman spectroscopy is a powerful analytical technique which can be used to analyze the composition of biological components in solution. It is becoming popular in biopharmaceutical production since it is non-invasive and allows for measurements to be taken in process thereby eliminating the need for sampling. This technique relies on measuring the spectrum of light scattered through a solution to measure protein concentration and quality attributes. 

The raw data used in this project includes 100 batches worth of processing data for the large-scale manufacutring of Insulin. Each batch includes many records of data from both sample measurements as well as Raman sepctra readings throughout the manufacturing of the Insulin product (for more info on the data set used please refer to [Data Sources](#data-sources) and [Acknowledgements](#acknowledgements)).

## Project Description
This project will focus specifically on the following columns from the 100_Batches_IndPenSim_V3.csv dataset:
- Time (h): time attribute measurements were taken (numeric)
- Penicillin concentration(P:g/L): measured concentration of Penicillin product sample (numeric)
- Batch ID: unique identifier of batch of Penicillin produced (integer)
- Fault Flag: indifier for any issues during Raman spec measurement (integer)
- {689-2089}: list of columns with Raman measurement data, the number corresponds to the light wavelength in nm used (numeric)

The goal of this project is to estimate the Penicillin concentration using only the Raman measurement data (from the 689-2089 nm wavelength columns) for each record (sampling point) and compare to the actual measured sample concentration (from the Penicillin concentration(P:g/L) column). The first 30 batches of the dataset will be used to train and test the model that will be used to calculate a Penicillin concentration. Each record from the final 30 batches will be ingested to the cloud in batch using airflow to simulate data being created in near real time. As each record is inserted, the estimated Penicillin concentration will be calculated by feeding the Raman measurement data to the model and the calculated result will be added to the dataset. The final feature data will be used to create a dashboard analyzing the accuracy of the model as well as the distribution of Insulin concentration between both sample measurements and model calculation results.

## Project Structure
The project is organized into the following directories:

0. `README.md`: Contains background and instructions for setting up and running the code
1. `airflow`: Contains the python scripts used to create DAGs and transfer data to/from Google Cloud
2. `data`: Stores raw and processed data files.
3. `model`: Contains all code related to model development (adapted from code originally created by Shashank Gupta, Ricardo Flores, and Rakesh Bobbala - see [Acknowledgements](#acknowledgements) for model development)

## Requirements
- Python 3.11
- Docker + Docker Compose
- Google Cloud Platform account and project
- Git

## Installation and Usage Instructions
0. It is recommended to run this in a Debian e2-standard-4 virtual machine with 100GB boot disk provided by google cloud compute and clone this repository into the home directory there. View [How to SSH into VM section](#how-to-ssh-into-vm) to set up your local machine to log into the virtual machine you created.

1. Clone this repository:
```
git clone https://github.com/delzio/de-bootcamp-project.git
```

2. Install [Docker Engine](https://docs.docker.com/engine/install/)

3. Download the raw data from [Kaggle (https://www.kaggle.com/datasets/stephengoldie/big-databiopharmaceutical-manufacturing)](https://www.kaggle.com/datasets/stephengoldie/big-databiopharmaceutical-manufacturing). You can use something like the below command to copy from your local computer to VM:
```
# Copy archive.zip to the raw folder, unzip it, and remove the old zip file (this may take a few tries but should work)
scp Downloads/archive.zip <username>@<vm name>:/home/<username>/de-bootcamp-project/data/raw
```

4. Move the compressed download file to <path_to_this_project>/data/raw and extract it there (it should create a new folder called Mendeley_data containing the csv files)

5. Raw csv file data must exist in the following path: <path_to_this_project>/data/raw/Mendeley_data/100_Batches_IndPenSim_V3.csv

6. Create or log in to your Google Cloud Platform account

7. Enable the compute engine, google cloud storage, and bigquery APIs (from the hamburger icon in the top left, go to APIs & Services -> Enabled APIs & services)

8. Create a new bucket in google cloud storage named what you like (will need to save the name for later)

9. Create a new dataset in BigQuery named what you like (will need to save the name for later)

10. Set up service account (from the hamburger icon go to IAM & Admin -> Service Accounts)

11. Create Service Account with your desired name and description

12. Recommended to grant just the Owner role but can grant the required specific roles if desired

13. Create the service account

14. Click the service account you created, go to keys, add a key and download as JSON

15. On the Linux VM (or other machine you intend to run this on), use the following commands and copy the contents of your GCP service account json key into the gcp.json file (replace <path_to_this_project> with the path to this git repo you cloned to your machine e.g. ~/de-bootcamp-project/.google/credentials/gcp.json):
    ```bash
    cd ~ && mkdir -p <path_to_this_project>/.google/credentials/
    touch <path_to_this_project>/.google/credentials/gcp.json
    ```

16. Move to the airflow folder and edit the .env_example file and replace the GCP env var values with the info from your GCP project and save as ".env" (make sure there are no added spaces in the variable names)

17. Configure airflow user id in .env file by running the following command:
    ```bash
    cd <path_to_this_project>/airflow/
    echo "AIRFLOW_UID=$(id -u)" >> .env
    ```

18. Move into the airflow folder path and start the airflow docker containers with:
    - docker-compose build
    - docker-compose up airflow-init
    - docker-compose up
    - (you may need to use "sudo docker compose" instead of "docker-compose")

19. Once the containers have finished launching, the data will immediately start being sent to your GCP GCS bucket and BigQuery dataset

20. If interested, you can view the DAG executions from the Airflow UI (user: airflow, pw: airflow) at http://localhost:8092/ (you may need to forward port 8092 over to your local computer if running from a VM)

21. You can pause, play, and manually trigger the DAGS from here

22. That's it! The first 30 batches of data should start flowing into your Google Cloud project in a few minutes and the final 70 batches of data will continuously be added via the DAGs until you shut down the containers

23. You can shut down the docker containers with
    ```bash
    docker-compose down
    ```

24. If you have any issues, you can open the Airflow UI and click on the dag tasks and check the logs to see error messages (most likely it is due to an incorrectly named environment parameter or missing credentials json)

## ETL Process
1. The raw_data_ingestion_gcs_dag is used to extract the data from the raw csv file and store raw data as a series of partitioned parquet files in to the raw path in a GCS Bucket in the first task

2. Once the files are sent to GCS, a second task is initiated which uses a standalone spark cluster to clean, format, and add contextualizers to the raw data from the parquet files and save them as cleaned parquet files to the processed path in a GCS Bucket

3. Once this is completed a third task is initiated to further clean and filter down the contextualized parquet files and saves the data for the first 30 batches worth of data to the T_SAMPLE_CONTEXT and T_RAMAN_CONTEXT BigQuery tables

4. This raw_data_ingestion_gcs_dag is intended to only run once to send the full data as parquet files and backfill the first 30 batches worth of data to the SQL tables

5. In parallel, the processed_to_bq_dag runs every 5 minutes continuously and searches for "new" batches from the processed parquet file data.

6. The first task of this DAG scans about 1-2 batches worth of data from the processed parquet files each time it is executed and further cleans and filters the data to continuously update the T_SAMPLE_CONTEXT and T_RAMAN_CONTEXT BigQuery tables

4. Once this task completes, the second task is triggered which takes the raman context data as input to a Pencillin prediction PLS model and joins the predicted concentration results to the actual sample concentration values from the sample context data.

5. It then sends the joined predicted and actual penicillin data to a new BigQuery table called T_RAMAN_PREDICTION

6. You can join the results from T_RAMAN_PREDICTION to T_SAMPLE_CONTEXT and T_RAMAN_CONTEXT to create a view of all final feature results data

7. The data from these tables was pulled into Google Data Looker Studio to create the [final dashboard for this project](https://lookerstudio.google.com/u/0/reporting/ce52f512-fd15-4ed5-8637-fc065f89c0c8/page/fRIxD)

## Database Schema
GCS:
- Raw Parquet files have all data from the csv file with an added "id" column representing the row number each sample measurement is from
- Processed Parquet files have only the relevant attribute data and ids with added artificial context columns (sample_ts and batch_number) to simulate real drug processing
BigQuery:
- T_SAMPLE_CONTEXT contains cleaned Penicillin sample concentration, ids, and context data from the processed parquet files partitioned by the sample timestamp (sample_ts) and clustered by the batch_number
- T_RAMAN_CONTEXT contains cleaned raman context data and ids from the processed parquet files partitioned by the sample timestamp (sample_ts)
- T_RAMAN_PREDICTION contains the final Penicillin actual sample concentration and model-predicted concentration and sample ids

## How to SSH into VM
1. Create a directory called .ssh in your local machine home path (~)

2. Create ssh keys from command line: 
    ```bash
    ssh-keygen -t rsa -f ~/.ssh/gcp -C USER -b 2048
    ```
3. This will create two key files in your ~/.ssh folder, copy the gcp.pub contents to GCP: "compute engine -> settings -> metadata -> ssh keys - copy public key"

4. connect using external ip

5. Create a file called config in ~/.ssh:
Host <gcp vm name>
    HostName <gcp vm external ip>
    User <your username>
    IdentityFile ~/.ssh/gcp

6. Can log on from your command line using "ssh <gcp vm name>" or from vs code using connect to host and selecting <gcp vm name>



## Author
- Jesse Delzio <jmdelzio@ucdavis.edu>

## License
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Acknowledgements
- This project was inspired by [2024 Data Engineering Zoomcamp](https://datatalks.club/blog/data-engineering-zoomcamp.html) offered by DataTalks.Club.
- The data used for this project was provided by [kaggle](https://www.kaggle.com/datasets/stephengoldie/big-databiopharmaceutical-manufacturing)
- The model development for this project was inspired by [Shashank Gupta, Ricardo Flores, and Rakesh Bobbala](https://www.kaggle.com/code/wrecked22/regression-analysis)

## List of useful GCP APIs
- Compute Engine API
- Cloud Logging API
- Analytics Hub API 					
- Artifact Registry API 					
- Backup for GKE API 					
- BigQuery API 					
- BigQuery Connection API 					
- BigQuery Data Policy API 					
- BigQuery Migration API 					
- BigQuery Reservation API 					
- BigQuery Storage API 					
- Cloud Autoscaling API 					
- Cloud Dataplex API 					
- Cloud Datastore API 					
- Cloud Deployment Manager V2 API 					
- Cloud DNS API 					
- Cloud Filestore API 					
- Cloud Monitoring API 					
- Cloud OS Login API 					
- Cloud Pub/Sub API 					
- Cloud Resource Manager API 					
- Cloud Run Admin API 					
- Cloud SQL 					
- Cloud SQL Admin API 					
- Cloud Storage 					
- Cloud Storage API 					
- Cloud Trace API 					
- Container File System API 					
- Container Registry API 					
- Dataform API 					
- Google Cloud APIs 					
- Google Cloud Storage JSON API 					
- IAM Service Account Credentials API 					
- Identity and Access Management (IAM) API 					
- Kubernetes Engine API 					
- Network Connectivity API 					
- Secret Manager API 					
- Serverless VPC Access API 					
- Service Management API 					
- Service Usage API 