#!/bin/bash

# Set environment variables
set -a
source ./airflow/.env
set +a

# Navigate to the terraform directory
cd ./terraform/ || { echo "Directory ./terraform/ not found. Make sure to run the script from the project source directory."; exit 1; }

# Delete bucket and dataset from GCP
echo "Deleting GCP storage bucket and dataset.."
terraform destroy