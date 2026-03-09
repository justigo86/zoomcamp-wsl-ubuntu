# Data Engineering Zoomcamp 2026
# Homework 6: Batch

## Question 1. Install Spark and PySpark
- Install Spark
- Run PySpark
- Create a local spark session

**What's the output?**
- Execute spark.version.

**ANSWER:**
4.1.1

## Question 2. Yellow November 2025
Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

**What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.**
- 6MB
- 25MB
- 75MB
- 100MB

**Script:**
```python
df = spark.read \
  .option("header", "true") \
  .parquet('yellow_tripdata_2025-11.parquet')
```
```python
df.repartition(4).write.parquet('data/pq/m6_hw_yellow_tripdata_2025-11')
```
**Command:**
```bash
ls -lh data/pq/m6_hw_yellow_tripdata_2025-11/
```

**ANSWER:**
25MB

**Explanation:**
```text
-rw-r--r-- 1 justigo86 justigo86 25M Mar  9 03:15 part-00000-8b8d63b7-66b2-4dd2-a2dd-2554a1187aad-c000.snappy.parquet
-rw-r--r-- 1 justigo86 justigo86 25M Mar  9 03:15 part-00001-8b8d63b7-66b2-4dd2-a2dd-2554a1187aad-c000.snappy.parquet
-rw-r--r-- 1 justigo86 justigo86 25M Mar  9 03:15 part-00002-8b8d63b7-66b2-4dd2-a2dd-2554a1187aad-c000.snappy.parquet
-rw-r--r-- 1 justigo86 justigo86 25M Mar  9 03:15 part-00003-8b8d63b7-66b2-4dd2-a2dd-2554a1187aad-c000.snappy.parquet
```

## Question 3. Count records

**How many taxi trips were there on the 15th of November?**
Consider only trips that started on the 15th of November.
- 62,610
- 102,340
- 162,604
- 225,768

**Script:**
```python
df_pq = spark.read.parquet('data/pq/m6_hw_yellow_tripdata_2025-11')
df_pq.printSchema()
```
```python
from pyspark.sql import functions as f
q3_count = df_pq.filter(f.to_date("tpep_pickup_datetime") == "2025-11-15").count()
q3_count
```

**ANSWER:**
162,604