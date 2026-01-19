#NOTE: code taken from Jupyter Notebook and converted into .py file
# file then updated from noteboook process into python script/function

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates
)

def insert_data():
    # parameterize user, password, host, port, db name, table name
    pg_user = "root"
    pg_pw = "root"
    pg_host = "localhost"
    pg_port = "5432"
    pg_db = "ny_taxi"
    table_name = "yellow_taxi_data"

    # parameterize yr and mo values in case we want to update them later
    year = 2021
    month = 1

    # due to size of dataset- need to break the read up into chunks
    chunksize = 100000

    # url variable
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"
    engine = create_engine(f'postgresql://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}')

    # df_iter = dataframe iterable variable
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )
    # iterator and chunksize are iterable values to create make the variable iterable

    # boolean var to check if table exists
    check = True
    # insert data with each loop
    for df_chunk in tqdm(df_iter):
        # check if table exists
        if check:
            # use .head().to_sql() in order to create the table - not adding data yet
            # helpful for large datasets - like this one
            df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            check = False
            print("Created table in database")
        # insert data into the table - one chunk at a time
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
        print("Inserted", len(df_chunk), "rows of data")

if __name__ == '__main__':
    insert_data()



# df = pd.read_csv(url) - ran first, but must update due to error
# For a description of the fields- see Yellow Trips Data Dictionary:
# https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# There is an error for "DtypeWarning: ... mixed types" on multiple columns
# this is because the data has values in cols that don't match the types inferred by read_csv()
# we must specify the types manually

# !uv add sqlalchemy psycopg2-binary
# - already ran - no need to install again

# !	- shell escape character - tells Jupyter to treat code as shell command rather than python code
# installing sqlalchemy and psycopg2 will install them in project toml file as dependency

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
# prints DDL schema
# shows the schema that's going to be unsed in out DB

# use tqdm to see data insert progress
# !uv add tqdm

# great way to make sure variable is iterable
# for df_chunk in df_iter:
#     print(len(df_chunk))