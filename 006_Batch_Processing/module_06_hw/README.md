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

## Question 4. Longest trip
**What is the length of the longest trip in the dataset in hours?**
- 22.7
- 58.2
- 90.6
- 134.5

**Script:**
```python
df_pq.registerTempTable('taxi_data')
```
```python
spark.sql("""
SELECT MAX ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600) AS max_hours
FROM taxi_data;
""").show()
```

**ANSWER:**
90.6

## Question 5. User Interface
**Spark's User Interface which shows the application's dashboard runs on which local port?**
- 80
- 443
- 4040
- 8080

**ANSWER:**
4040

**Explanation:**
Link: https://spark.apache.org/docs/latest/configuration.html
```text
spark.ui.port - 4040 - Port for your application's dashboard, which shows memory and workload data.
```

## Question 6: Least frequent pickup location zone
Load the zone lookup data into a temp view in Spark:
```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```
**Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?**
- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

**Script:**
```python
df_zones = spark.read \
  .option("header", "true") \
  .csv('taxi_zone_lookup.csv')
```
```python
df_joined = df_pq.join(df_zones, df_pq.PULocationID == df_zones.LocationID)
```
```python
# using Python
df_joined.groupBy("Zone") \
  .count() \
  .select("Zone","count") \
  .orderBy("count") \
  .show(5, truncate=False)
```
OR
```python
# to use SQL
df_joined.registerTempTable('joined_data')
```
```sql
spark.sql("""
SELECT Zone, COUNT(*)
FROM joined_data
GROUP BY Zone
ORDER BY COUNT(*)
LIMIT 5;
;
""").show()
```

**ANSWER:**
- Arden Heights = 1
- Governor's Island/Ellis Island/Liberty Island = 1
- Eltingville/Annadale/Prince's Bay = 1

**Explanation:**
Chose Arden Heights because it appears first on the list despite sharing count with two other zones.