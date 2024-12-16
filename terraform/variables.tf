# User Specific Variables
variable "project" {
    description = "My Project"
}

variable "region" {
    description = "My Project Region"
}

variable "location" {
    description = "My Project Location"
}

variable "bucket" {
    description = "My Storage Bucket Name"
}

# Fixed Variables
variable "dataset" {
    description = "My BigQuery Dataset Name"
}

variable "credentials" {
    description = "My Project Credentials"
    default = "../.google/credentials/gcp.json"
}

variable "gcs_storage" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

