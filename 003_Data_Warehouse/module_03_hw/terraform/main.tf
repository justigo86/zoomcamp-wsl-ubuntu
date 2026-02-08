terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# create GCS bucket
resource "google_storage_bucket" "m3-bq-hw-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# create BQ dataset
resource "google_bigquery_dataset" "m3-bq-hw-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# run .py script to upload data in GCS bucket
resource "null_resource" "upload_data" {
  # This triggers every time the bucket is created
  provisioner "local-exec" {
    command = "python ${path.module}/load_yellow_taxi_data.py --bucket=${google_storage_bucket.m3-bq-hw-bucket.name}"
  }

  # Ensures the script only runs AFTER the bucket is created
  depends_on = [google_storage_bucket.m3-bq-hw-bucket]
}