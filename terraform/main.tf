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

## Assign roles to service account
#resource "google_project_iam_member" "owner" {
#  project = "your-project-id"
#  role    = "roles/Owner"
#  member  = "serviceAccount:${var.service_account_email}"
#}
#
#resource "google_project_iam_member" "storage" {
#  project = "your-project-id"
#  role    = "roles/Storage Admin"
#  member  = "serviceAccount:${var.service_account_email}"
#}


# Resources required for Storage Buckets and BigQuery
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
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
  dataset_id = var.bq_dataset_name
  location = var.location
  delete_contents_on_destroy = true
}


# Resources required for dataproc cluster creation
resource "google_compute_subnetwork" "subnetwork-default" {
  name          = "mysubnetwork"
  ip_cidr_range = "10.2.0.0/16"
  region        = var.region
  network       = google_compute_network.network-default.id
  secondary_ip_range {
    range_name    = "tf-test-secondary-range-update1"
    ip_cidr_range = "192.168.10.0/24"
  }
  private_ip_google_access = true  # Enable Private Google Access
}

resource "google_compute_network" "network-default" {
  name                    = "mynetwork"
  auto_create_subnetworks = false
}

resource "google_compute_firewall" "allow_internal" {
  name    = "allow-internal"
  network = google_compute_network.network-default.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = ["10.2.0.0/16"]
}

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
      image_version = "2.2-debian12"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      tags = ["foo", "bar"]
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]
      #network = google_compute_network.network-default.name
      subnetwork = google_compute_subnetwork.subnetwork-default.name
    }

    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
      timeout_sec = 500
    }
  }
}