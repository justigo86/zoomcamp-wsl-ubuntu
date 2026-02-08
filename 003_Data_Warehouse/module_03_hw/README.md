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

## BigQuery Setup
Create an external table using the Yellow Taxi Trip Records.
```sql
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-m3-bq-bucket1674/yellow_tripdata_2024-*.parquet']
);
```

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
```sql
CREATE OR REPLACE TABLE `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized` AS
SELECT * FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`;
```

## Question 1. Counting records
**What is count of records for the 2024 Yellow Taxi Data?**
- 65,623
- 840,402
- 20,332,093
- 85,431,289

**Commands:**
```sql
SELECT COUNT(1) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**ANSWER:**
20,332,093