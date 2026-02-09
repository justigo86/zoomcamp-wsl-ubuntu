-- Creating external table using parquet files uploaded to GCS bucket
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`   --save to my project folder structure
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-m3-bq-bucket1674/yellow_tripdata_2024-*.parquet']
);

SELECT * FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata` LIMIT 10;

-- Create a materialized table from external table
CREATE OR REPLACE TABLE `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized` AS
SELECT * FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`;

-- Q1: What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(1) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;

-- Q2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT PULocationID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.external_yellow_tripdata`;
SELECT COUNT(DISTINCT PULocationID) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;

-- Q3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery.
SELECT PULocationID FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table.
SELECT PULocationID, DOLocationID FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized`;

-- Q4: How many records have a fare_amount of 0?
SELECT COUNT(1) FROM `zoomcamp-m3-bq.m3_bq_hw_dataset.yellow_tripdata_materialized` WHERE fare_amount  = 0;
