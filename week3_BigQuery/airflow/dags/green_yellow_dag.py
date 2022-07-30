import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
<<<<<<< HEAD
# from airflow.operators.python import PythonOperator

# from google.cloud import storage
from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
=======
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
>>>>>>> fd19692a4bd88b5b2790a905443036d545ab5437
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
<<<<<<< HEAD
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
=======
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'colour_trips_data')
>>>>>>> fd19692a4bd88b5b2790a905443036d545ab5437

DATASET = "tripdata"
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
# INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
<<<<<<< HEAD
URL_TEMPLATE = URL_PREFIX  + {colour} +' _tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = {colour} + '_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
OUTPUT_CSV_FILE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

=======
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

>>>>>>> fd19692a4bd88b5b2790a905443036d545ab5437
default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
<<<<<<< HEAD
    dag_id="green_yellow_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['green_yellow'],
=======
    dag_id="green_yellow_gcs_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['green_yellow_gcs'],
>>>>>>> fd19692a4bd88b5b2790a905443036d545ab5437
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        download_dataset_task = BashOperator(
<<<<<<< HEAD
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {path_to_local_home}/{OUTPUT_FILE_TEMPLATE}"
    )

        # move_files_gcs_task = GCSToGCSOperator(
        #     task_id=f'move_{colour}_{DATASET}_files_task',
        #     source_bucket=BUCKET,
        #     source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
        #     destination_bucket=BUCKET,
        #     destination_object=f'{colour}/{colour}_{DATASET}',
        #     move_object=True
        # )

        # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        #     task_id=f"bq_{colour}_{DATASET}_external_table_task",
        #     table_resource={
        #         "tableReference": {
        #             "projectId": PROJECT_ID,
        #             "datasetId": BIGQUERY_DATASET,
        #             "tableId": f"{colour}_{DATASET}_external_table",
        #         },
        #         "externalDataConfiguration": {
        #             "autodetect": "True",
        #             "sourceFormat": f"{INPUT_FILETYPE.upper()}",
        #             "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
        #         },
        #     },
        # )

        # CREATE_BQ_TBL_QUERY = (
        #     f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
        #     PARTITION BY DATE({ds_col}) \
        #     AS \
        #     SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        # )

        # # Create a partitioned table from external table
        # bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        #     task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
        #     configuration={
        #         "query": {
        #             "query": CREATE_BQ_TBL_QUERY,
        #             "useLegacySql": False,
        #         }
        #     }
        # )

download_dataset_task
        # move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
=======
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
>>>>>>> fd19692a4bd88b5b2790a905443036d545ab5437
