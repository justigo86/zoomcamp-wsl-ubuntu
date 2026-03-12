import pandas as pd
from models import ride_from_row, ride_serializer
from kafka import KafkaProducer
import time

def pythonProducerFunc():

  url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
  columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
  df = pd.read_parquet(url, columns=columns).head(1000)
  topic_name = 'rides'

  # updated producer serializer
  producer = KafkaProducer(
      bootstrap_servers=['localhost:9092'],
      value_serializer=ride_serializer
  )

  # time for tracking purposes
  t0 = time.time()

  # iterate over rows to send data - pulling pieces of code from above
  for _, row in df.iterrows():
      ride = ride_from_row(row)
      producer.send(topic_name, value=ride)
      print(f"Sent: {ride}")
      time.sleep(0.01)

  producer.flush()

  t1 = time.time()
  return print(f'took {(t1 - t0):.2f} seconds')