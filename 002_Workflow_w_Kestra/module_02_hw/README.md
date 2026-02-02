# Data Engineering Zoomcamp 2026
# Module 2: Workflow Orchestration with Kestra
# Module 2 Homework


## Question 1.
**Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?**
- 128.3 MiB
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

**Solution:**
Two ways:
After running 08_gcp_taxi.yaml with taxi=yellow, year=2020, month=12 inputs:
1. Checked Kestra UI under Executions => zoomcamp.08_gcp_taxi => Ouutputs => extract => outputFiles => yellow_tripdata_2020-12.csv.
2. Checked GCS Cloud Storage -> Buckets => [my_bucket_name] => yellow_tripdata_2020-12.csv has size of 134.5MB - converts to 128.3 MiB.

**Answer:**
128.3 MiB


## Question 2.
**What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?**
- {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
- green_tripdata_2020-04.csv
- green_tripdata_04_2020.csv
- green_tripdata_2020.csv

**Solution:**
From 08_gcp_taxi.yaml:
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
- updated to answer with "{{render(vars.file)}}"

**Answer:**
green_tripdata_2020-04.csv


## Question 3.
**How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?**
- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

**Solution:**
After running 09_gcp_taxi_scheduled.yaml with backfill for all yellow taxi data between 2019-01 - 2021-07:
```sql
SELECT COUNT(1)
FROM `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2020%';
```

**Answer:**
24,648,499


## Question 4.
**How many rows are there for the Green Taxi data for all CSV files in the year 2020?**
- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

**Solution:**
After running 09_gcp_taxi_scheduled.yaml with backfill for all green taxi data between 2019-01 - 2021-07:
```sql
SELECT COUNT(1)
FROM `zoomcamp-m2-kestra.zoomcamp.green_tripdata`
WHERE filename LIKE 'green_tripdata_2020%';
```

**Answer:**
1,734,051


## Question 5.
**How many rows are there for the Yellow Taxi data for the March 2021 CSV file?**
- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

**Solution:**
After running 09_gcp_taxi_scheduled.yaml with backfill for all yellow taxi data between 2019-01 - 2021-07:
```sql
SELECT COUNT(1)
FROM `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2021-03.csv';
```

**Answer:**
1,925,152

## Question 6.
**How would you configure the timezone to New York in a Schedule trigger?**
- Add a timezone property set to EST in the Schedule trigger configuration
- Add a timezone property set to America/New_York in the Schedule trigger configuration
- Add a timezone property set to UTC-5 in the Schedule trigger configuration
- Add a location property set to New_York in the Schedule trigger configuration

**Solution:**
https://kestra.io/docs/workflow-components/triggers/schedule-trigger

**Answer:**
Add a timezone property set to America/New_York in the Schedule trigger configuration