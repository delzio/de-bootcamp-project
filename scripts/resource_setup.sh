#!/bin/bash

# Navigate to the terraform directory
cd ./terraform/ || { echo "Directory ./terraform/ not found. Make sure to run the script from the project source directory."; exit 1; }

# Initialize Terraform
echo "Initializing Terraform..."
terraform init

# Create Terraform Plan
terraform plan

# User can check plan for GCP resource creation and must confirm to create resources
read -p "Press Enter to continue with terraform apply (or Ctrl-C to quit)..."

# Apply Terraform configuration
echo "Applying Terraform configuration..."
terraform apply -auto-approve

# Copy spark scripts to GCS bucket
source ./airflow/.env
gcloud auth activate-service-account --key-file=./.google/credentials/gcp.json
gsutil cp ./airflow/dags/*.py gs://$GCP_GCS_BUCKET/pyspark_code/
gsutil cp ./data/processed/model/raman_pls_model.pk1 gs://$GCP_GCS_BUCKET/model/raman_model.pk1


