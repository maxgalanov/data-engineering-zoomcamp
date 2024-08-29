import pandas as pd
import os
from sqlalchemy import create_engine
from time import time


def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name, csv_file)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print("Connection established")

    start_time = time()

    df_iter = pd.read_csv(csv_file, compression='gzip', iterator=True, chunksize=100_000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name=table_name, con=engine, if_exists='replace')

    end_time = time()

    print(f"Inserted first chunk, took {end_time - start_time} seconds")

    while True:
        try:
            start_time = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            end_time = time()

            print(f"Inserted another chunk, took {end_time - start_time} seconds")
            
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break