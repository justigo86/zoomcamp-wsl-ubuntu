#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__file__


# In[3]:


prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
url = f"{prefix}yellow_tripdata_2021-01.csv.gz"
url


# In[4]:


# df = pd.read_csv(url) - ran first, but must update due to error
# For a description of the fields- see Yellow Trips Data Dictionary:
# https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# There is an error for "DtypeWarning: ... mixed types" on multiple columns
# this is because the data has values in cols that don't match the types inferred by read_csv()
# we must specify the types manually

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


# In[5]:


df.head()


# In[6]:


df.shape


# In[7]:


df.dtypes


# In[15]:


# !uv add sqlalchemy psycopg2-binary
# - already ran - no need to install again

# !	- shell escape character - tells Jupyter to treat code as shell command rather than python code
# installing sqlalchemy and psycopg2 will install them in project toml file as dependency


# In[8]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[9]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
# prints DDL schema
# shows the schema that's going to be unsed in out DB


# In[10]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
# use .head().to_sql() in order to create the table - not adding data yet
# helpful for large datasets - like this one


# In[22]:


# due to size of dataset- need to break the read up into chunks
# df_iter = dataframe iterable variable
df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)
# iterator and chunksize are iterable values to create make the variable iterable


# In[21]:


# great way to make sure variable is iterable
for df_chunk in df_iter:
    print(len(df_chunk))


# In[13]:


# use tqdm to see data insert progress
get_ipython().system('uv add tqdm')


# In[14]:


from tqdm.auto import tqdm


# In[23]:


# insert data with each loop
for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




