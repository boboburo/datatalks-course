import os

import logging

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

import datetime as datetime

#paths for downloading data
#https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
#https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-01.csv

URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
OUTPUT_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/raw_csv/taxi_zone_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/parquet/taxi_zone_{{ execution_date.strftime(\'%Y-%m\')}}.parquet'

#GCP parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
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


# create the dag

gcs_workflow = DAG(
    dag_id = "ZONES_GCSIngestionDag",
    schedule_interval = "0 6 1 * *", 
    start_date=datetime.datetime(2021, 1, 1),
    end_date=datetime.datetime(2021,1,2),
    )


with gcs_workflow:
    wget_task = BashOperator(
        task_id = "wget",
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_CSV_FILE_TEMPLATE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_CSV_FILE_TEMPLATE,
            "dest_file": OUTPUT_PARQUET_FILE_TEMPLATE
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "zones/" + OUTPUT_PARQUET_FILE_TEMPLATE.split("/")[-1],
            "local_file": OUTPUT_PARQUET_FILE_TEMPLATE,
        },
    )

    wget_task >> format_to_parquet_task >> local_to_gcs_task
