import pandas as pd
import click
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pw', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='taxi_hw', help='PostgreSQL database name')
def insert_data(
    pg_user,
    pg_pw,
    pg_host,
    pg_port,
    pg_db,
):
    green_taxi_data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    zone_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(green_taxi_data_url)
    zone_df = pd.read_csv(zone_url)

    df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')
    zone_df.to_sql(name='zones_data', con=engine, if_exists='replace')

    print("Inserted", len(df), "rows of data for table green_taxi_data")
    print("Inserted", len(zone_df), "rows of data for table zones_data")

if __name__ == '__main__':
    insert_data()