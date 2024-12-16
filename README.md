# Data Engineering Zoomcamp Final Project: ETL Pipeline for Penicillin Production Data Analysis

## Introduction
This project uses the techniques learned throughout participation in the DataTalks.Club 2024 data engineering zoomcamp course to extract, transform, and load data from the large-scale manufacturing of Penicillin into a dashboard for analysis. 

This project is intended to serve as a quick-to-setup demonstration of a practical use case for data engineering tools in the field of biotechnology.

Project Evaluation Criteria Notes:

Problem Description can be found throughout this README in the [Background](#background), [Project Description](#project-description), [ETL Process](#etl-process), and [Database Schema](#database-schema) sections

Google Cloud Platform was used to develop this project

Data Ingestion was performed using batch pipelines consististing of multiple inter-dependent dags each with multiple tasks uploading data to GCS buckets and BigQuery tables (view [ETL Process](#etl-process))

Biquery was used for data warehousing processed data. Table partitioning and clustering was applied to multiple tables (view [Database Schema](#database-schema))

Spark was used for complex data transformations

Looker Studio was used to create the visualization dashboard

This project should be fully reproduceable through docker using the [Installation and Usage Instructions](#installation-and-usage-instructions) (it is recommended to follow these instructions using a VM hosted by GCP)

## Data Architecture
![de-zoomcamp-project drawio](https://github.com/user-attachments/assets/4cecfd06-0444-4196-b161-4b65b5fe5ab4)

## Data Visualization
![RamanModelingDashboard](https://github.com/user-attachments/assets/0fcd25c8-3bc4-4a50-acee-85fe9e7725ef)


## Background
Penicillin is a vital group of antibiotics used in the treatment of various bacterial infections. It is produced through fermentation of the Penicillium fungus and is later purified from these fungal cells and all other impurities. To understand the amount of Penicillin produced, samples are typically taken at several intervals throughout the manufacturing process. This sampling is invasive, leading to delays in manufacturing and increased risk of contaminations. Raman spectroscopy is a powerful analytical technique which can be used to analyze the composition of biological components in solution. It is becoming popular in biopharmaceutical production since it is non-invasive and allows for measurements to be taken in-process thereby eliminating the need for sampling. This technique relies on measuring the spectrum of light scattered through a solution to measure target product concentration and quality attributes. 

The raw data used in this project includes 100 batches worth of processing data for the large-scale manufacutring of Penicillin. Each batch includes many records of data from both sample measurements as well as Raman sepctra readings throughout the manufacturing of the Penicillin product (for more info on the data set used please refer to [Data Sources](#data-sources) and [Acknowledgements](#acknowledgements)).

## Project Description
This project will focus specifically on the following columns from the 100_Batches_IndPenSim_V3.csv dataset:
- Time (h): time attribute measurements were taken (numeric)
- Penicillin concentration(P:g/L): measured concentration of Penicillin product sample (numeric)
- Batch ID: unique identifier of batch of Penicillin produced (integer)
- Fault Flag: indifier for any issues during Raman spec measurement (integer)
- {689-2089}: list of columns with Raman measurement data, the number corresponds to the light wavelength in nm used (numeric)

The goal of this project is to estimate the Penicillin concentration using only the Raman measurement data (from the 689-2089 nm wavelength columns) for each record (sampling point) and compare to the actual measured sample concentration (from the Penicillin concentration(P:g/L) column). The first 30 batches of the dataset will be used to train the model that will be used to calculate a Penicillin concentration. Each record from the final 30 batches will be ingested to the cloud in batch using airflow to simulate data being created in near real time. As each record is inserted, the estimated Penicillin concentration will be calculated by feeding the Raman measurement data to the model and the calculated result will be added to the dataset. The final feature data will be used to create a dashboard analyzing the accuracy of the model as well as the distribution of Insulin concentration between both sample measurements and model calculation results.

## Project Structure
The project is organized into the following directories:

0. `README.md`: Contains background and instructions for setting up and running the code
1. `airflow`: Contains the python scripts used to create DAGs and transfer data to/from Google Cloud
2. `data`: Stores raw and processed data files.
3. `model`: Contains all code related to model development (adapted from code originally created by Shashank Gupta, Ricardo Flores, and Rakesh Bobbala - see [Acknowledgements](#acknowledgements) for model development)
4. `terraform`: Contains the terraform code for building GCP resources required for the project
5. `scripts`: Contains bash scripts that linux machine users can follow to build and start the project
6. `lib`: Contains spark and hadoop jar fars needed to run spark locally

## Requirements
- Python 3.11
- Docker + Docker Compose
- Google Cloud Platform account and project
- Git

## Installation and Usage Instructions
0. WARNING: Please keep in mind that because this project is intended to be used as a quick proof of concept, Google Cloud accesses and permissions are assigned very loosely. You may consider deviating from the recommended instructions here to apply more strict rules to your GCP resources.

1. In your Google Cloud Platform project activate the following APIs:
 - Compute Engine API
 - Cloud Dataproc API
 - Cloud Storage API
 - BigQuery API

2. Create a Service Account with your desired name and description (from the hamburger icon go to IAM & Admin -> Service Accounts)

3. Recommended to simply grant Owner role to the service account but can grant the minimum required specific roles if desired for extra security

4. Go to Compute Engine, create a new VM isntance, create a Debian e2-standard-4 virtual machine, update the boot disk size to 100GB in the OS and Storage section, update the service account to the one you just created and set the access scopes to "Allow full access to all Cloud APIs" in the security section. View [How to SSH into VM](#how-to-ssh-into-vm) section to set up your local machine to log into the virtual machine you created.

5. Once the service account is created, click on it, go to keys, add a key and download as JSON 

6. Install git and SSH into the cloud compute virtual machine and clone this repository:
```bash
sudo apt update && sudo apt install git
git clone -b main --single-branch https://github.com/delzio/de-bootcamp-project.git
```

7. Create a file named gcp.json to store your Google crednetials using the following terminal commands (replace <path_to_project_home> with the path to this git repo you cloned to your machine e.g. ~/de-bootcamp-project/.google/credentials/gcp.json):
```bash
mkdir -p <path_to_project_home>/.google/credentials/
touch <path_to_project>/.google/credentials/gcp.json
```

8. Copy the contents of the service account key JSON you downloaded into the gcp.json file
```bash
# Can open the gcp.json file to paste in the key contents with:
nano <path_to_this_project>/.google/credentials/gcp.json
```

9. Move to the project home directory (de-bootcamp-project folder). We can quickly setup the project by running the scripts in ./scripts  (must be in project home path to run scripts successfully).

10. Run the 0_install_software.sh script to install docker and terraform on the virtual machine
```bash
bash ./scripts/0_install_software.sh
```

11. Confirm that Docker and Docker Compose were successfully installed by running the terminal commands below (Install Docker following the [Docker installation docs here](https://docs.docker.com/engine/install/) if not successfully installed via script)
```bash
docker version
# it's ok if you get this error here: permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock
docker compose version
```

12. Confirm that Terraform was successfully installed by running the terminal command below (Install Terraform following the [Terraform installation docs here](https://developer.hashicorp.com/terraform/install) if not successfully installed via script)
```bash
terraform version
```

13. Download the raw data as zip from [Kaggle (https://www.kaggle.com/datasets/stephengoldie/big-databiopharmaceutical-manufacturing)](https://www.kaggle.com/datasets/stephengoldie/big-databiopharmaceutical-manufacturing). You can use something like the below command to copy from your local computer to VM:
```bash
# Copy archive.zip to the raw folder, unzip it, and remove the old zip file (this may take a few tries but should work)
scp Downloads/archive.zip <username>@<vm name>:/home/<username>/de-bootcamp-project/data/raw
```

14. Unzip the raw data file, and remove the old zip file from data/raw directory in your virtual machine
```bash
# unzip is a linux library that should be installed from the 0_install_software.sh script
unzip archive.zip
rm archive.zip
```

15. Confirm that csv file data exists in the following path (move to this path if not): <path_to_project_home>/data/raw/Mendeley_data/100_Batches_IndPenSim_V3.csv

16. Move to the airflow folder and edit the .env_example file and replace the GCP env var values with the info from your GCP project and save as ".env" (make sure there are no added spaces in the variable names)

17. Move back to project home path and run the 1_resource_setup.sh script to create your GCP resources
```bash
bash ./scripts/1_resource_setup.sh
```

18. Run the 2_airflow_start.sh script to start the airflow service and begin the data pipeline processes
```bash
bash ./scripts/2_airflow_start.sh
```

19. Once the containers have finished launching, the data will immediately start being sent to your GCP GCS bucket and BigQuery dataset

20. If interested, you can view the DAG executions from the Airflow UI (user: airflow, pw: airflow) at http://localhost:8092/ (you may need to forward port 8092 over to your local computer if running from a VM)

21. You can view progress of dags and their tasks along with any logs or error messages sent by the dags from here. You can also pause, play, and manually trigger the DAGS from here if you want to explore, but they should be setup to pull in all data and populate BigQuery tables with model predictions automatically.

22. That's it! The first 30 batches of data should start flowing into your Google Cloud project in a few minutes and the final 70 batches of data will continuously be added via the DAGs until you shut down airflow

23. You can shut down airflow with
```bash
bash ./scripts/3_airflow_stop.sh
```

24. You can delete all of the GCP resources created for this project (Bucket and Dataset) with:
```bash
bash ./scripts/4_resource_destroy.sh
```

25. If you have any issues, you can open the Airflow UI and click on the dag tasks and check the logs to see error messages (most likely it is due to an incorrectly named environment parameter or missing credentials json)

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

7. The data from these tables was pulled into Google Data Looker Studio to create the [visualization dashboard](#data-visualization)

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
    ssh-keygen -t rsa -f ~/.ssh/gcp -C <YourUsername> -b 2048
    ```
3. This will create two key files in your ~/.ssh folder, copy the gcp.pub contents to GCP: "compute engine -> settings -> metadata -> ssh keys - copy public key"

4. connect using external ip

5. Create a file called config in ~/.ssh:
```bash
Host <gcp vm name>
    HostName <gcp vm external ip>
    User <YourUsername>
    IdentityFile ~/.ssh/gcp
```

6. Can log on from your command line using "ssh <gcp vm name>" or from vs code using connect to host and selecting <gcp vm name>


## Author
- Jesse Delzio <delziojesse2@gmail.com>

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
