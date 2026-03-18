# Data Engineering Zoomcamp 2026
# Homework 7: Streaming

## Setup

Used the same infrastructure from the [workshop](../../../07-streaming/workshop/).

Followed the setup instructions. Built the Docker image, started the services, and performed a clean start with the existing image:
```bash
docker compose down -v
docker compose build
docker compose up -d
```

## Question 1. Redpanda version
Run `rpk version` inside the Redpanda container:
```bash
docker exec -it workshop-redpanda-1 rpk version
```

**What version of Redpanda are you running?**

**ANSWER:**
v25.3.9

## Question 2. Sending data to Redpanda
Create a topic called `green-trips`:
```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```
Now write a producer to send the green taxi data to this topic.
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

Read the parquet file and keep only these columns:
- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

```python
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)
```

Convert each row to a dictionary and send it to the `green-trips` topic.
```python
def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )
```

You'll need to handle the datetime columns - convert them to strings before serializing to JSON.
```python
ride = ride_from_row(df.iloc[0])
ride
def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=ride_serializer
)

producer.send(topic_name, value=ride)
producer.flush()
```

Measure the time it takes to send the entire dataset and flush:
```python
from time import time
t0 = time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")

producer.flush()
t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

**How long did it take to send the data?**
- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

**ANSWER:**
10 seconds

**Explanation:**
```text
took 17.45 seconds
```

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic (set `auto_offset_reset='earliest'`).
```python
from kafka import KafkaConsumer
from dataclasses import dataclass
import json

#same as producer
server = 'localhost:9092'
topic_name = 'green-trips'

# create a class to ensure datatypes utilized
@dataclass
class Ride:
    lpep_pickup_datetime: int
    lpep_dropoff_datetime: int
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)


consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',  # reading from beginning - default is latest
    group_id='green-trips-console',  # creates group ID for tracking purposes / continuity
    value_deserializer=ride_deserializer
)

def run_trips_count():
  print(f"Listening to {topic_name}...")

  count = 0
  for message in consumer:
      ride = message.value
      if ride.trip_distance > 5.0:
          count += 1
          print(f"\n Number of trips greater than 5.0km: {count}")

  consumer.close()

run_trips_count()
```

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

**How many trips have `trip_distance` > 5?**
- 6506
- 7506
- 8506
- 9506

**ANSWER:**
8506

**Explanation:**
```text
 Number of trips greater than 5.0km: 8506
```

## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with
the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables
for your results.

## Question 4. Tumbling window - pickup location
Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM q4_events
ORDER BY num_trips DESC
LIMIT 3;
```

**Which `PULocationID` had the most trips in a single 5-minute window?**
- 42
- 74
- 75
- 166

**ANSWER:**
74

**Explanation:**
```text
+--------------+-----------+
| pulocationid | num_trips |
|--------------+-----------|
| 74           | 15        |
| 74           | 14        |
| 74           | 13        |
+--------------+-----------+
```

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

**How many trips were in the longest session?**
- 12
- 31
- 51
- 81

**Query:**
```sql
SELECT * FROM q5_events ORDER BY num_trips DESC LIMI
 T 3;
 ```

 **ANSWER:**
 81

 **Explanation:**
 ```text
 +---------------------+---------------------+--------------+-----------+
| window_start        | window_end          | pulocationid | num_trips |
|---------------------+---------------------+--------------+-----------|
| 2025-10-08 06:46:14 | 2025-10-08 08:27:40 | 74           | 81        |
| 2025-10-01 06:52:23 | 2025-10-01 08:23:33 | 74           | 72        |
| 2025-10-22 06:58:31 | 2025-10-22 08:25:04 | 74           | 71        |
+---------------------+---------------------+--------------+-----------+
```


## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

**Which hour had the highest total tip amount?**
- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

**Query:**
```sql
SELECT * FROM q6_events ORDER BY tip_total DESC LIMI
 T 3;
 ```

**ANSWER:**
2025-10-16 18:00:00

**Explanation:**
```text
+---------------------+---------------------+--------------------+
| window_start        | window_end          | tip_total          |
|---------------------+---------------------+--------------------|
| 2025-10-16 18:00:00 | 2025-10-16 19:00:00 | 510.8599999999999  |
| 2025-10-16 17:00:00 | 2025-10-16 18:00:00 | 445.01000000000005 |
| 2025-10-24 17:00:00 | 2025-10-24 18:00:00 | 433.31             |
+---------------------+---------------------+--------------------+
```