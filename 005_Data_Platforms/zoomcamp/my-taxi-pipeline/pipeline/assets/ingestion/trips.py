"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.12

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy - suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: VendorID
    data_type: int
    description: "TPEP provider. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc."

  - name: tpep_pickup_datetime
    data_type: timestamp
    description: "Date and time when the meter was engaged"

  - name: tpep_dropoff_datetime
    data_type: timestamp
    description: "Date and time when the meter was disengaged"

  - name: passenger_count
    data_type: int
    description: "Number of passengers"

  - name: trip_distance
    data_type: float
    description: "Trip distance in miles"

  - name: RatecodeID
    data_type: int
    description: "1= Standard rate; 2= JFK; 3= Newark; 4= Nassau or Westchester; 5= Negotiated fare; 6= Group ride"

  - name: store_and_fwd_flag
    data_type: string
    description: "trip record held in vehicle memory before sent to the vendor. Y= store and forward; N= not store and forward"

  - name: PULocationID
    data_type: int
    description: "pickup Location"

  - name: DOLocationID
    data_type: int
    description: "drop-off Location"

  - name: payment_type
    data_type: int
    description: "how passenger paid for the trip. 1= Credit card; 2= Cash; 3= No charge; 4= Dispute; 5= Unknown; 6= Voided trip"

  - name: fare_amount
    data_type: float
    description: "time-and-distance fare calculated by the meter"

  - name: extra
    data_type: float
    description: "extras and surcharges. e.g., rush hour and overnight charges"

  - name: mta_tax
    data_type: float
    description: "$0.50 MTA tax - automatically triggered based on metered rate"

  - name: tip_amount
    data_type: float
    description: "Tip amount - cash tips not included"

  - name: tolls_amount
    data_type: float
    description: "total amount of all tolls paid"

  - name: improvement_surcharge
    data_type: float
    description: "$0.30 improvement surcharge"

  - name: total_amount
    data_type: float
    description: "total amount charged to passengers"

  - name: congestion_surcharge
    data_type: float
    description: "surcharge Manhattan congestion zone."

  - name: taxi_type
    type: string
    description: "type of taxi (e.g., yellow, green, fhv)"

  - name: file_month
    type: timestamp
    description: "month for which file was ingested"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
import os
import json
import pandas as pd

def materialize():
    # Bruin automatically injects these environment variables
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    
    # Extract variables defined in your pipeline.yml or .bruin.yml
    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    # 1. Generate list of months between start and end dates
    # 'MS' is the frequency for 'Month Start'
    months = pd.date_range(start=start_date, end=end_date, freq='MS')

    all_dataframes = []

    # 2. Loop through each taxi type and each month to fetch files
    for taxi_type in taxi_types:
        for date_val in months:
            year = date_val.year
            # Ensure month is zero-padded (e.g., '01' instead of '1')
            month = f"{date_val.month:02d}"
            
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                print(f"Fetching: {url}")
                # pandas.read_parquet supports HTTP URLs directly
                df = pd.read_parquet(url)
                
                # Optional: Add columns to track source for lineage/debugging
                df['taxi_type'] = taxi_type
                df['file_month'] = pd.to_datetime(date_val)
                
                all_dataframes.append(df)
            except Exception as e:
                # Useful if a specific month is missing on the Cloudfront server
                print(f"Could not fetch {url}: {e}")

    # 3. Combine everything into one final DataFrame
    if not all_dataframes:
        return pd.DataFrame()

    final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    return final_dataframe

    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe


