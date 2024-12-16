#!/bin/bash

# Navigate to the terraform directory
cd ./airflow/ || { echo "Directory ../airflow/ not found. Make sure to run the script from the project source directory."; exit 1; }

# stop airflow services
sudo docker compose down