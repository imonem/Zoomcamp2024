#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system(f'wget {url} -O {parquet_name}')

    df = pd.read_parquet(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(con=engine, name=table_name, chunksize=100000, if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the result to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)