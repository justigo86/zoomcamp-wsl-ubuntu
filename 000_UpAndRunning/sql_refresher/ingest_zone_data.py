import pandas as pd
import click
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pw', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='taxi_zone_data', help='Target table name')
def insert_data(
    pg_user,
    pg_pw,
    pg_host,
    pg_port,
    pg_db,
    target_table
):
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    engine = create_engine(f'postgresql://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}')
    df = pd.read_csv(url)
    df.to_sql(name=target_table, con=engine, if_exists='replace')
    print("Inserted", len(df), "rows of data")


if __name__ == '__main__':
    insert_data()