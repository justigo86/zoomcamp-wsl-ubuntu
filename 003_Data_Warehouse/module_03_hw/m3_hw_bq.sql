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
