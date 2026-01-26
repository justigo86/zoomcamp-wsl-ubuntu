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
db:5432
Explanation:
- db is the service name of the pg database container
- 5432 is the internal port of the postgres container


## Environment Setup for questions 3-6
pg database container:
```bash
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="green_taxi_trips" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
```

pgadmin container:
```bash
docker run -it --rm \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --name my-pgadmin \
  --network pg-network \
  dpage/pgadmin4
```