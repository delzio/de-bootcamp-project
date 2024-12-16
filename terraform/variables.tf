variable "credentials" {
    description = "My Project Credentials"
    default = "../.google/credentials/gcp.json"
}

variable "project" {
    description = "My Project"
}

variable "region" {
    description = "My Project Region"
}

variable "location" {
    description = "My Project Location"
}

variable "service_account_email" {
    description = "My Cluster Service Account"
}

variable "bucket" {
    description = "My Storage Bucket Name"
}

variable "dataset" {
    description = "My BigQuery Dataset Name"
}

variable "gcs_storage" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "cluster_name" {
    description = "My Dataproc Cluster"
    default = "spark-cluster"
}


