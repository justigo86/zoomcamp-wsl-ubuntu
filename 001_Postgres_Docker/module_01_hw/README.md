# Data Engineering Zoomcamp 2026
# Homework 1: Docker, SQL and Terraform


## Question 1. Understanding Docker Images
Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container.

**What's the version of pip in the python:3.13 image?**
- 25.3
- 24.3.1
- 24.2.1
- 23.3.1

**Commands:**
```bash
docker run -it --rm --entrypoint=bash python:3.13
pip -V
```

**Answer:**
25.3


## Question 2. Understanding Docker networking and docker-compose
Given the following docker-compose.yaml
```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data
  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin
volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

**What is the hostname and port that pgadmin should use to connect to the postgres database?**

**Answer:**
db:5432

Explanation:
- db is the service name of the pg database container
- 5432 is the internal port of the postgres container


## After environment Setup for questions 3-6
## Question 3. Counting short trips
**For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?**

**Commands:**
```sql
SELECT COUNT(1)
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
AND lpep_pickup_datetime < '2025-12-01'
AND trip_distance <= 1;
```

**Answer:**
8007

## Question 4. Longest trip for each day
**Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).**
**Use the pick up time for your calculations.**

**Commands:**
```sql
SELECT DATE(lpep_pickup_datetime)
FROM green_taxi_data
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1;
```

**Answer:**
2025-11-14

## Question 5. Biggest pickup zone
**Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?**

**Commands:**
```sql
SELECT zd."Zone"
FROM zones_data zd, green_taxi_data gtd
WHERE zd."LocationID" = gtd."PULocationID"
  AND gtd.lpep_pickup_datetime >= '2025-11-18'
  AND gtd.lpep_pickup_datetime < '2025-11-19'
GROUP BY zd."Zone"
ORDER BY SUM(gtd.total_amount) DESC
LIMIT 1;
```

**Answer:**
East Harlem North

## Question 6. Largest tip
**For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?**
**Note: it's tip , not trip. We need the name of the zone, not the ID.**

**Commands:**
```sql
SELECT zdo."Zone", gtd.tip_amount
FROM green_taxi_data gtd
JOIN zones_data zdo ON zdo."LocationID" = gtd."DOLocationID"
JOIN zones_data zpu ON zpu."LocationID" = gtd."PULocationID"
WHERE zpu."Zone" = 'East Harlem North'
  AND gtd.lpep_pickup_datetime >= '2025-11-01'
  AND gtd.lpep_pickup_datetime < '2025-12-01'
ORDER BY gtd.tip_amount DESC
LIMIT 1;
```

**Answer:**
Yorkville West

## Question 7. Terraform Workflow
**Which of the following sequences, respectively, describes the workflow for:**
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

**Answer:**
terraform init, terraform apply -auto-approve, terraform destroy