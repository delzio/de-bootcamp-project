#!/bin/bash

# Set airflow user id variable in .env if not already there
grep -q '^AIRFLOW_UID=' ./airflow/.env || echo  "AIRFLOW_UID=$(id -u)" >> ./airflow/.env

# Set environment variables
set -a
source ./airflow/.env
set +a

# Navigate to the terraform data stores directory
cd ./terraform/ || { echo "Directory ./terraform/ not found. Make sure to run the script from the project source directory."; exit 1; }

# Initialize Terraform
echo "Initializing Terraform..."
terraform init

# Create Terraform Plan
terraform plan

# User can check plan for GCP resource creation and must confirm to create resources
read -p "Press Enter to continue with terraform apply for gcs and bigquery (or Ctrl-C to quit)..."

# Apply Terraform configuration
echo "Applying Terraform configuration..."
terraform apply -auto-approve

# Copy spark scripts, model file, and dataproc scripts to GCS bucket
cd ..
gcloud auth activate-service-account --key-file=./.google/credentials/gcp.json
gsutil cp ./airflow/dags/pyspark_helpers/* gs://$TF_VAR_bucket/pyspark_code/
gsutil cp ./airflow/dags/dag_functions.py gs://$TF_VAR_bucket/pyspark_code/
gsutil cp ./data/processed/model/raman_pls_model.pk1 gs://$TF_VAR_bucket/model/raman_model.pk1