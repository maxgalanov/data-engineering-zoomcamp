import os
import pandas as pd
import re
import logging
import pyarrow as pa
import pyarrow.parquet as pq

from sqlalchemy import create_engine
from time import time
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
URL = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
OUTPUT_FILE = AIRFLOW_HOME + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
TRANSFORMED_FILE = OUTPUT_FILE.rstrip(".csv.gz") +"_transformed.csv"

PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_SCHEMA = "mage"
TABLE_NAME = "green_taxi"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GSP_TABLE_NAME = "green_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


def transform_data(input_file: str, output_file: str):
    if input_file.endswith(".csv.gz"):
        df = pd.read_csv(input_file, compression='gzip')
    elif input_file.endswith(".csv"):
        df = pd.read_csv(input_file)
    else:
        raise Exception("Only .csv files could be proccesed")
    
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]

    df.loc[:, 'lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date

    df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower(), inplace=True)

    assert 'vendor_id' in df.columns, "Column 'vendor_id' does not exist in the DataFrame."
    assert (df['passenger_count'] > 0).all(), "There are entries with passenger_count <= 0."
    assert (df['trip_distance'] > 0).all(), "There are entries with trip_distance <= 0."

    print("All assertions passed successfully!")

    df.to_csv(output_file, index=False)

    print(f"Transformed data saved to {output_file}")

    return


# def ingest_callable(user, password, host, port, db, schema_name, table_name, csv_file):
#     print(schema_name, table_name, csv_file)

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
#     engine.connect()

#     print("Connection established")

#     df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100_000)

#     while True:
#         try:
#             start_time = time()

#             df = next(df_iter)

#             df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='append')

#             end_time = time()

#             print(f"Inserted another chunk, took {end_time - start_time} seconds")
            
#         except StopIteration:
#             print("Finished ingesting data into the postgres database")
#             break

def convert_csv_to_parquet(csv_file_path, parquet_file_path):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)
    
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    # Convert DataFrame to Apache Arrow Table
    table = pa.Table.from_pandas(df)
    # Write Table to Parquet file
    pq.write_table(table, parquet_file_path)


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def delete_local_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted local file: {file_path}")
    else:
        print(f"File not found: {file_path}")


def export_data_to_google_cloud_storage(file_name, bucket, catalog_name, table_name) -> None:
    destination_blob_name = f'{catalog_name}/{table_name}'
    local_parquet_path = file_name.rstrip(".csv") + '.parquet'
    
    convert_csv_to_parquet(file_name, local_parquet_path)
    
    upload_to_gcs(bucket, local_parquet_path, destination_blob_name)
    
    delete_local_file(local_parquet_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 1),
    "end_date": datetime(2020, 12, 31),
    "depends_on_past": False,
    "retries": 1,
}

green_taxi_workflow = DAG(
    dag_id="green_taxi_etl",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['de_zoomcamp']
)

with green_taxi_workflow:
    
    # Downloading data from source
    load_task = BashOperator(
        task_id='download_csv_data',
        bash_command=f"curl -sSL {URL} > {OUTPUT_FILE}"
    )

    # Transforming loaded data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs = dict(
            input_file=OUTPUT_FILE,
            output_file=TRANSFORMED_FILE
        )
    )

    # # Upload trandformed data to postgres
    # ingest_postgres_task = PythonOperator(
    #     task_id='ingest_postgres',
    #     python_callable=ingest_callable,
    #     op_kwargs = dict(
    #         user=PG_USER,
    #         password=PG_PASSWORD,
    #         host=PG_HOST,
    #         port=PG_PORT,
    #         db=PG_DATABASE,
    #         schema_name=PG_SCHEMA,
    #         table_name=TABLE_NAME,
    #         csv_file=TRANSFORMED_FILE
    #     )
    # )

    # Upload trandformed data to postgres
    ingest_gcs_task = PythonOperator(
        task_id='ingest_gcs',
        python_callable=export_data_to_google_cloud_storage,
        op_kwargs = dict(
            file_name=TRANSFORMED_FILE,
            bucket=BUCKET,
            catalog_name=TABLE_NAME,
            table_name=GSP_TABLE_NAME
        )
    )

    load_task >> transform_task >> ingest_gcs_task
    # load_task >> transform_task >> ingest_postgres_task
    # transform_task >> ingest_gcs_task