terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
# creds has been set through the GOOGLE_APPLICATION_CREDENTIALS environment variable.
# To reproduce, see setup.sh
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "ecopulse-datalake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "ecopulse-dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
}