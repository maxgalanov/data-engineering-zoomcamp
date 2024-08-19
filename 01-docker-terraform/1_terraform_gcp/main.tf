terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.41.0"
    }
  }
}

provider "google" {
  project = "friendly-idea-433003-e4"
  region  = "us-central1"
}

resource "google_storage_bucket" "de-course-bucket" {
  name          = "friendly-idea-433003-e4-de-course-bucket"
  location      = "US"
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