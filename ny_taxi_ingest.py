#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres.')

# user
# password
# host
# port
# database name
# table name
# url of parquet

parser.add_argument('user', help='username for postgres')
parser.add_argument('password', help='password for postgres')
parser.add_argument('host', help='hostname for postgres')
parser.add_argument('port', help='port for postgres')
parser.add_argument('table_name', help='name of the table where we will write the result to')
parser.add_argument('url', help='url of the parquet file')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'
    
    # download the parquet
    
    args = parser.parse_args()
    print(args.accumulate(args.integers))

    df = pd.read_parquet(f'{url}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df.head(n=0).to_sql(name=f'{table_name}', con=engine, if_exists='replace')

    df.to_sql(con=engine, name=f'{table_name}', chunksize=100000, if_exists="append")

# print(pd.DataFrame.to_sql(df, con=engine, name='yellow_taxi_data', chunksize=100000, if_exists="append"))
