import types as t

t.StructType([
  t.StructField('VendorID', t.IntegerType(), True), 
  t.StructField('lpep_pickup_datetime', t.TimestampType(), True), 
  t.StructField('lpep_dropoff_datetime', t.TimestampType(), True), 
  t.StructField('store_and_fwd_flag', t.StringType(), True), 
  t.StructField('RatecodeID', t.IntegerType(), True), 
  t.StructField('PULocationID', t.IntegerType(), True), 
  t.StructField('DOLocationID', t.IntegerType(), True), 
  t.StructField('passenger_count', t.IntegerType(), True), 
  t.StructField('trip_distance', t.DoubleType(), True), 
  t.StructField('fare_amount', t.DoubleType(), True), 
  t.StructField('extra', t.DoubleType(), True), 
  t.StructField('mta_tax', t.DoubleType(), True), 
  t.StructField('tip_amount', t.DoubleType(), True), 
  t.StructField('tolls_amount', t.DoubleType(), True), 
  t.StructField('ehail_fee', t.DoubleType(), True), 
  t.StructField('improvement_surcharge', t.DoubleType(), True), 
  t.StructField('total_amount', t.DoubleType(), True), 
  t.StructField('payment_type', t.IntegerType(), True), 
  t.StructField('trip_type', t.IntegerType(), True), 
  t.StructField('congestion_surcharge', t.DoubleType(), True)
])