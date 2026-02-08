# Data Engineering Zoomcamp 2026
# Homework 3: BigQuery and Data Warehouse

## Homework Setup
Used Terraform to create a GCS bucket (1), run load_yellow_taxi_data.py to upload the 2024 taxi data (2), and create a BigQuery dataset to query the data (3).
1. resource "google_storage_bucket" "m3-bq-hw-bucket"
2. resource "null_resource" "upload_data"
3. resource "google_bigquery_dataset" "m3-bq-hw-dataset"
Then set credentials using the following command:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="[path_to_module_homework_directory]/terraform/keys/my-creds.json"
```
Lastly, ran terraform init/plan/apply to create the infrastructure and upload data.

