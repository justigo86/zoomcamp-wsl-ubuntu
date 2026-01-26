terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "zoomcamp-m1-terraform"
  region  = "us-central1"
}

resource "google_storage_bucket" "tf-demo-bucket" {
  name          = "zoomcamp-m1-terraform-tf-demo-bucket"
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