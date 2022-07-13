import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'colour_trips_data')

DATASET = "tripdata"
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
# INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
DATASET = "tripdata"
URL_SUFFIX = '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# URL_TEMPLATE = URL_PREFIX  + {colour} +' _tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = '_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
OUTPUT_CSV_FILE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="green_yellow_gcs_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['green_yellow_gcs'],
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        download_dataset_task = BashOperator(
        task_id=f'download_{colour}_{DATASET}_dataset_task',
        bash_command=f"curl -sSL {URL_PREFIX}{colour}{URL_SUFFIX} >> {path_to_local_home}/{colour}{URL_SUFFIX}"
    )
        local_to_gcs_task = PythonOperator(
        task_id=f"local_{colour}_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{colour}_{DATASET}/{colour}{URL_SUFFIX}",
            "local_file": f"{path_to_local_home}/{colour}{URL_SUFFIX}",
        },
    )

download_dataset_task >> local_to_gcs_task
