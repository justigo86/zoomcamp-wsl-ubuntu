-- video: https://www.youtube.com/watch?v=B-WtpB0PuG4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=29

-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
-- FROM `taxi-rides-ny.nytaxi.yellow_tripdata_partitioned` WHERE fare_amount != 0;
FROM `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0;

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
-- CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.yellow_tripdata_ml` (
CREATE OR REPLACE TABLE `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml` (
    -- create the schema for the table
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
    -- then select data and populate the table with data to train the model
  SELECT passenger_count, trip_distance, CAST(PULocationID AS STRING), CAST(DOLocationID AS STRING),
  CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  -- FROM `taxi-rides-ny.nytaxi.yellow_tripdata_partitioned` WHERE fare_amount != 0
  FROM `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTINGS
    -- linear regression model to predict tip amount
    -- uses 80% of data for training and 20% for evaluation
    -- takes multiple minutes to build
-- CREATE OR REPLACE MODEL `taxi-rides-ny.nytaxi.tip_model`
CREATE OR REPLACE MODEL `zoomcamp-m2-kestra.zoomcamp.tip_model`
OPTIONS      -- define the 'brain' of the model
  (model_type='linear_reg',     -- required - specify type of model
  input_label_cols=['tip_amount'],     -- required - specify the column to predict
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS       -- optional - specify how to split data for training and evaluation
SELECT * FROM      -- create the training data for the model - pull data and feed it to model
  -- `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
  `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml`
WHERE
  tip_amount IS NOT NULL;

-- CHECK FEATURES - provided by BQ for the created model
-- SELECT * FROM ML.FEATURE_INFO(MODEL `taxi-rides-ny.nytaxi.tip_model`);
SELECT * FROM ML.FEATURE_INFO(MODEL `zoomcamp-m2-kestra.zoomcamp.tip_model`);
  -- feature info provided by BQ - shows the features used for training the model and their types

-- EVALUATE THE MODEL - like a practice test
  -- compare predicted values from model against the actual values
SELECT * FROM
  -- ML.EVALUATE(MODEL `taxi-rides-ny.nytaxi.tip_model`,
  ML.EVALUATE(MODEL `zoomcamp-m2-kestra.zoomcamp.tip_model`,
(
  SELECT * FROM     -- evaluation data fed to the model
  -- `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
  `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
));

-- PREDICT THE MODEL - used to make guesses on new data
SELECT * FROM
  -- ML.PREDICT(MODEL `taxi-rides-ny.nytaxi.tip_model`,
  ML.PREDICT(MODEL `zoomcamp-m2-kestra.zoomcamp.tip_model`,
(
  SELECT * FROM   -- new data set fed to model to make predictions
  -- `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
  `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
));

-- PREDICT AND EXPLAIN
SELECT * FROM
  -- ML.EXPLAIN_PREDICT(MODEL `taxi-rides-ny.nytaxi.tip_model`,
  ML.EXPLAIN_PREDICT(MODEL `zoomcamp-m2-kestra.zoomcamp.tip_model`,
(
  SELECT * FROM
  -- `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
  `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

-- HYPER PARAM TUNNING
-- CREATE OR REPLACE MODEL `taxi-rides-ny.nytaxi.tip_hyperparam_model`
CREATE OR REPLACE MODEL `zoomcamp-m2-kestra.zoomcamp.tip_hyperparam_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT * FROM
  -- `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
  `zoomcamp-m2-kestra.zoomcamp.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;
