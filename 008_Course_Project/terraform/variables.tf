variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "zoomcamp-final-project-491003"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "zoomcamp-proj-bq-bucket0326"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "proj_bq_dataset"
}