# Data Engineering Zoomcamp 2026
# Homework 4: Analytics Engineering

## Homework Setup


## Question 1. dbt Lineage and Execution
Given a dbt project with the following structure:
```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

**If you run dbt run --select int_trips_unioned, what models will be built**
- stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)
- Any model with upstream and downstream dependencies to int_trips_unioned
- int_trips_unioned only
- int_trips_unioned, int_trips, and fct_trips (downstream dependencies)

**ANSWER:**
int_trips_unioned only

**Explanation:**
From dbt docs: docs.getdbt.com/reference/node-selection/syntax
"Use the --select flag with dbt run to select a subset of models to run. Note that the following arguments (--select, --exclude, and --selector) also apply to other dbt tasks, such as test and build."
"dbt run --select "my_dbt_model"          # runs a specific model"

## Question 2. dbt Tests
You've configured a generic test like this in your schema.yml:
```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```
Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.

**What happens when you run dbt test --select fct_trips?**
- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

**ANSWER:**
dbt will fail the test, returning a non-zero exit code

**Explanation:**
From dbt docs: https://docs.getdbt.com/best-practices/writing-custom-generic-tests
"If a nonzero number of records is returned instead, then at least one record in model is odd, and the test has failed."

## Question 3. Counting Records in fct_monthly_zone_revenue
After running your dbt project, query the fct_monthly_zone_revenue model.

**What is the count of records in the fct_monthly_zone_revenue model?**
- 12,998
- 14,120
- 12,184
- 15,421

**Command:**
```sql
SELECT COUNT(1) FROM prod.fct_monthly_zone_revenue;
```

**ANSWER:**
12,184

## Question 4. Best Performing Zone for Green Taxis (2020)
Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.

**Which zone had the highest revenue?**
- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

**Command:**
```sql
SELECT pickup_zone, revenue_monthly_total_amount
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' AND YEAR(revenue_month) = 2020
ORDER BY revenue_monthly_total_amount DESC
LIMIT 1;
```

**ANSWER:**
East Harlem North

## Question 5. Green Taxi Trip Counts (October 2019)

**Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?**
- 500,234
- 350,891
- 384,624
- 421,509

**CommandL**
```sql
SELECT SUM(total_monthly_trips)
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
AND revenue_month >= '2019-10-01'
AND revenue_month <='2019-10-31';
```

**Answer**
384,624

## Question 6. Build a Staging Model for FHV Data
Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.
1. Load the FHV trip data for 2019 into your data warehouse
2. Create a staging model stg_fhv_tripdata with these requirements:
  - Filter out records where dispatching_base_num IS NULL
  - Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)

**What is the count of records in stg_fhv_tripdata?**
- 42,084,899
- 43,244,693
- 22,998,722
- 44,112,187

**Command:**

**Answer:**

