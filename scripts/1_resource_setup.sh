#!/bin/bash

# Set airflow user id variable in .env if not already there
grep -q '^AIRFLOW_UID=' ./airflow/.env || echo  "AIRFLOW_UID=$(id -u)" >> ./airflow/.env


# Set environment variables
set -a
source ./airflow/.env
set +a

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


