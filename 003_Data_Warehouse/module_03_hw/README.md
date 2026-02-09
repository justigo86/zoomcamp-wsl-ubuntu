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

**Command:**
```sql
SELECT COUNT(1) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**ANSWER:**
20,332,093


## Question 2. Data read estimation
**Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.**
**What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table


**Commands:**
```sql
-- 1 - external:
SELECT COUNT(DISTINCT PULocationID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`;
-- 2 - materialized:
SELECT COUNT(DISTINCT PULocationID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**ANSWER:**
0 MB for the External Table and 155.12 MB for the Materialized Table

## Question 3: Understanding columnar storage
**Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?**
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed. <<--
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

**Commands:**
```sql
-- 1 - PULocationID:
SELECT PULocationID FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
-- 2 - PULocationID and DOLocationID
SELECT PULocationID, DOLocationID FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**ANSWER:**
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4: Counting zero fare trips
**How many records have a fare_amount of 0?**
- 128,210
- 546,578
- 20,188,016
- 8,333

**Command:**
```sql
SELECT COUNT(1) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized` WHERE fare_amount  = 0;
```
**Answer:**
8,333

## Question 5. Partitioning and clustering
**What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)**
- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

**Command:**
```sql
CREATE OR REPLACE TABLE `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**Answer:**
Partition by tpep_dropoff_datetime and Cluster on VendorID

## Question 6. Partition benefits
**Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)**
**Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?**
- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

**Commands:**
```sql
-- 1 - materiablized table:
SELECT COUNT(DISTINCT VendorID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized` WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- 2 - paritioned and clustered table:
SELECT COUNT(DISTINCT VendorID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_partitioned_clustered` WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

**Answer:**
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7. External table storage
**Where is the data stored in the External Table you created?**
- Big Query
- Container Registry
- GCP Bucket
- Big Table

**Answer:**
GCP Bucket

**Explanation:**
Data is not read from its own internal DB, it reaches out to a source, in this case, the GCP bucket.

## Question 8. Clustering best practices
**It is best practice in Big Query to always cluster your data:**
- True
- False

**Answer:**
False

**Explanation:**
While clustering can be beneficial, for things like performance/cost, it's not always necessary. 

## Question 9. Understanding table scans
**Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?**

**Command:**
```sql
SELECT COUNT(*) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
```

**Response:**
'0 B' - Due to being a count query, it's the amount provided by the metadata. 