#!/bin/bash

# Navigate to the terraform directory
cd ./airflow/ || { echo "Directory ./airflow/ not found. Make sure to run the script from the project source directory."; exit 1; }

# start airflow services
echo "Building Docker images..."
sudo docker compose build
echo "Launching airflow..."
sudo docker compose up airflow-init
sudo docker compose up