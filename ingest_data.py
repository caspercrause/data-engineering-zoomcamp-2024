#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from re import match
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user       = params.user
    password   = params.password
    host       = params.host 
    port       = params.port 
    db         = params.db
    table_name = params.table_name
    url        = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # Parse all date time columns:
    datecols = [x for x in df.columns if match('.*(datetime).*', x) ]
    for col in datecols:
        df[col] = pd.to_datetime(df[col])

    # make all headers lower case:
    
    df.columns = df.columns.str.lower()

    # print(pd.io.sql.get_schema(df, name='yellow_taxi_trips', con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') # Create the table without any data

    df.to_sql(name=table_name, con=engine, if_exists='append') # insert new rows if the table exists


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)
            # Parse all date time columns:
            datecols = [x for x in df.columns if match('.*(datetime).*', x) ]
            for col in datecols:
                df[col] = pd.to_datetime(df[col])

            # make all headers lower case:
            df.columns = df.columns.str.lower()

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second(s)' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)