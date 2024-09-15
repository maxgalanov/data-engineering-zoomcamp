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
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
OUTPUT_FILE = AIRFLOW_HOME + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
TRANSFORMED_FILE = OUTPUT_FILE.rstrip(".parquet") +"_transformed.parquet"

CATALOG_NAME = "green_tripdata"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GSP_FILE_NAME = "green_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


def transform_data(input_file: str, output_file: str):
    if input_file.endswith(".parquet"):
        df = pd.read_parquet(input_file)
    else:
        raise Exception("Only .parquet files could be proccesed")
    
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]

    df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower(), inplace=True)

    assert 'vendor_id' in df.columns, "Column 'vendor_id' does not exist in the DataFrame."
    assert (df['passenger_count'] > 0).all(), "There are entries with passenger_count <= 0."
    assert (df['trip_distance'] > 0).all(), "There are entries with trip_distance <= 0."

    print("All assertions passed successfully!")

    table = pa.Table.from_pandas(df)

    pq.write_table(table, output_file)

    print(f"Transformed data saved to {output_file}")

    return


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


def export_data_to_google_cloud_storage(file_name, bucket, catalog_name, gs_file_name) -> None:
    destination_blob_name = f'{catalog_name}/{gs_file_name}'
    local_parquet_path = file_name
    
    upload_to_gcs(bucket, local_parquet_path, destination_blob_name)
    
    delete_local_file(local_parquet_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "end_date": datetime(2023, 12, 31),
    "depends_on_past": False,
    "retries": 1,
}

green_taxi_workflow = DAG(
    dag_id="green_taxi_etl_2022",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de_zoomcamp', 'week_4']
)

with green_taxi_workflow:
    
    # Downloading data from source
    load_task = BashOperator(
        task_id='download_parquet_data',
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

    ingest_gcs_task = PythonOperator(
        task_id='ingest_gcs',
        python_callable=export_data_to_google_cloud_storage,
        op_kwargs = dict(
            file_name=TRANSFORMED_FILE,
            bucket=BUCKET,
            catalog_name=CATALOG_NAME,
            gs_file_name=GSP_FILE_NAME
        )
    )

    load_task >> transform_task >> ingest_gcs_task