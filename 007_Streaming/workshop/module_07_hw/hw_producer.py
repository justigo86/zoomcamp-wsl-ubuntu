import pandas as pd
from kafka import KafkaProducer
from dataclasses import dataclass
import json
import dataclasses

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

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

def pythonProducerFunc():
  url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
  columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
  df = pd.read_parquet(url, columns=columns)
  topic_name = 'green-trips'

  # updated producer serializer
  producer = KafkaProducer(
      bootstrap_servers=['localhost:9092'],
      value_serializer=ride_serializer
  )

  # iterate over rows to send data - pulling pieces of code from above
  for _, row in df.iterrows():
      ride = ride_from_row(row)
      producer.send(topic_name, value=ride)
      print(f"Sent: {ride}")

  producer.flush()

pythonProducerFunc()