terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project = var.project
  region  = var.region
}


# Resources required for Storage Buckets and BigQuery
resource "google_storage_bucket" "demo-bucket" {
  name          = var.bucket
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1 #days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.dataset
  location = var.location
  delete_contents_on_destroy = true
}

# Resources required for Dataproc Compute Cluster
resource "google_dataproc_cluster" "mycluster" {
  name     = var.cluster_name
  region   = var.region
  graceful_decommission_timeout = "120s"
  labels = {
    foo = "bar"
  }

  cluster_config {
    #staging_bucket = "dataproc-staging-bucket"

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     = "n2-standard-2"
      disk_config {
        boot_disk_size_gb = 30
        num_local_ssds    = 1
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
        "dataproc:pip.packages" = "chemotools==0.1.5"
      }
    }

    endpoint_config {
      enable_http_port_access = "true"
    }

    gce_cluster_config {
      tags = ["foo", "bar"]
      internal_ip_only = "false"
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]
    }

    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
      timeout_sec = 500
    }
  }
}