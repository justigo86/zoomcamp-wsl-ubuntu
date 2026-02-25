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

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import os
import json
import pandas as pd

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

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


