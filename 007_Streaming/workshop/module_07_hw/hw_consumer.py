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