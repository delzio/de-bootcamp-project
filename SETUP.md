# Setup Instructions

These instructions will guide you through setting up and installing the project requirements on a Linux machine.

## Prerequisites

It is recommended to use a Linux VM from Google Cloud Platform (free trial offered for 90 days) to run this project. Instructions for GCP Linux VM setup:

1. If you do not have a GCP account, sign up using the 90 day free trial and create a project
2. From the hamburger icon in the top left go to Compute Engine -> VM Instances
3. Create an instance with your desired name and region
4. Recommend to use the e2-standard-4 preset machine option
5. Recommend to use Boot disk options Debian 12, balanced presistent disk, 100 GB size
6. Create the instance
7. You should now be able to connect to the instance by clicking the SSH option while the instance is running
8. WARNING: please familiarize yourself with the billing charges associated with running a VM in the cloud. The GCP 90 day trial offers $300 credit and this project should come nowhere near reaching that limit, but make sure to turn off your VM instance(s) when not in use.

Setup GCP service account for project:

1. From the hamburger icon in the top left, go to APIs & Services -> Enabled APIs & services and ensure the [required GCP APIs are enabled](#list-of-required-gcp-apis)
2. From the hamburger icon go to IAM & Admin -> Service Accounts
3. Create Service Account with your desired name and description
4. Recommended to grant just the Owner role but can grant the required specific roles if desired
5. Create the service account
6. Click the service account you created, go to keys, add a key and download as JSON
7. On the Linux VM (or other Linux machine where you intend to run the project), run the following commands and copy the contents of your GCP service account json key into the google_credentials.json file:
    ```bash
    cd ~ && mkdir -p ~/.google/credentials/
    touch ~/.google/credentials/google_credentials.json
    ```

You will need to install the following on your machine:

- Python 3.x (comes installed with Linux machines, check version with ```python3 --version```)
- pip (comes installed with Linux machines, check version with ```pip--version```)
- Virtualenv 
- Docker and Docker Compose v2.x
- Terraform
- Airflow

## Clone Repo and Install Libraries

1. Clone the repository to your machine's home directory:

    ```bash
    git clone https://github.com/delzio/insulin-de-project.git
    ```

2. Navigate to the project directory:

    ```bash
    cd insulin-de-project
    ```

3. Create and activate a virtual environment (optional but recommended):

    ```bash
    virtualenv venv
    source venv/bin/activate
    ```

4. Install the project dependencies using pip:

    ```bash
    pip install -r requirements.txt
    ```

## Docker Setup

1. Install Docker and Docker Compose on the Linux VM:
    ```bash
    # Add Docker's official GPG key:
    sudo apt-get update
    sudo apt-get install ca-certificates curl
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    # Add the repository to Apt sources:
    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update
    # install latest docker engine and compose
    sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    ```
2. Ensure the installation worked properly by running the hello-world image:
    ```bash
    sudo docker run hello-world
    ```
3. Docker and Docker Compose have now been installed, can check versions with:
    ```bash
    sudo docker version
    sudo docker compose version
    ```

## Airflow Setup

1. Configure airflow user id in .env file:
    ```bash
    echo "AIRFLOW_UID=$(id -u)" >> .env
    ```

2. Update the GCP_PROJECT_ID and GCP_GCS_BUCKET values in `.env_example` to your project info and save the file as `.env`

3. 

## Configuration

1. Copy the sample configuration file:

    ```bash
    cp config.sample.yaml config.yaml
    ```

2. Update the `config.yaml` file with your specific configurations for the project.

## Running the Project

Now that you have installed and configured the project, you can run it:


## List of Required GCP APIs
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
