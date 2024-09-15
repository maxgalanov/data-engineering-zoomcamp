import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    dag_id="LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    catchup=True
)

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    
    wget_task = BashOperator(
        task_id="wget",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs = dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        )
    )

    wget_task >> ingest_task