import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator

# from google.cloud import storage
from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET = "tripdata"
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
# INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
URL_TEMPLATE = URL_PREFIX  + {colour} +' _tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = {colour} + '_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
OUTPUT_CSV_FILE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="green_yellow_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['green_yellow'],
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        download_dataset_task = BashOperator(
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