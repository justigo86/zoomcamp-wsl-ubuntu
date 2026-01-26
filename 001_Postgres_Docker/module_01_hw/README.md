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
SELECT COUNT(1) FROM green_taxi_data WHERE lpep_pi
 ckup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01' AND
   trip_distance <= 1;
```

**Answer:**
8007