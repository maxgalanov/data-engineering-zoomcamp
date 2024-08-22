terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.41.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = "friendly-idea-433003-e4"
  region      = "us-central1"
}

resource "google_storage_bucket" "de-course-bucket" {
  name          = "friendly-idea-433003-e4-de-course-bucket"
  location      = var.bq_location
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

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
}