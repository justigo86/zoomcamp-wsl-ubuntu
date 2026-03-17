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