#!/usr/bin/env python
# coding: utf-8

import os
import csv
import argparse
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO


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

    # df.to_sql(con=engine, name=table_name, chunksize=100000, if_exists='append')
    psql_insert_copy(table=table_name, conn=engine, keys=df.head(n=0), data_iter=df)

# Alternative to_sql() *method* for DBs that support COPY FROM

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('user', help='username for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='hostname for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('table_name', help='name of the table where we will write the result to')
    parser.add_argument('url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)