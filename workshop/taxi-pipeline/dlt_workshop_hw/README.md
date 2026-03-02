# Data Engineering Zoomcamp 2026
# Workshop Homework: Ingestion with dlt

## Homework setup
```bash
mkdir taxi-pipeline
cd taxi-pipeline
dlt init dlthub:taxi_pipeline duckdb
```
created taxi_pipeline-docs.yaml using gemini ai for understanding doc.yaml files in this context and to generate file
prompt agent to generate api source
```Please generate a REST API Source for NYC taxi data, as specified in @taxi_pipeline-docs.yaml
Place the code in taxi_pipeline.py and name the pipeline taxi_pipeline.
Use @dlt rest api as a tutorial.
After adding the endpoints, allow user to run the pipeline with python taxi_pipeline.py and await further instructions.
```
ran pipeline
```bash
python taxi_pipeline.py
```
opened dlt dashboard
```bash
dlt pipeline taxi_pipeline show
```

## Question 1. What is the start date and end date of the dataset?
- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01

**Command:**
```sql
SELECT
  MIN(trip_dropoff_date_time) AS start_date,
  MAX(trip_pickup_date_time) AS end_date
FROM nyc_yellow_taxi_trips;
```

**ANSWER:**
2009-06-01 to 2009-07-01


## Question 2. What proportion of trips are paid with credit card?
- 16.66%
- 26.66%
- 36.66%
- 46.66%

**Command:**
```sql
SELECT 
    AVG(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) * 100 AS credit_payment_percentage
FROM nyc_yellow_taxi_trips;
```

**ANSWER:**
26.66%

## Question 3. What is the total amount of money generated in tips?
- $4,063.41
- $6,063.41
- $8,063.41
- $10,063.41

**Command:**
```sql
SELECT
  SUM(tip_amt)
FROM "nyc_yellow_taxi_trips"
```

**ANSWER:**
$6,063.41