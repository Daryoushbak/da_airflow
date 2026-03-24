import os
import sys

# Fix for Windows: os.register_at_fork is only available on Unix systems
if not hasattr(os, 'register_at_fork'):
    os.register_at_fork = lambda **kwargs: None

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from google.cloud import bigquery, storage

# HTTP config
INFOKUBEN_URL = os.getenv("AIRFLOW_CONN_INFOKUB_URL")
INFOKUBEN_USERNAME = os.getenv("AIRFLOW_CONN_INFOKUB_USERNAME")
INFOKUBEN_PASSWORD = os.getenv("AIRFLOW_CONN_INFOKUB_PASSWORD")

# GCP config
service_account_info = json.loads(os.environ["GCP_SA_KEY"])

GCS_BUCKET = "ur-pipeline-testing"
GCS_PREFIX = "raw/infokuben"
GCS_FILENAME_PREFIX = "TV_IQ"
GCS_FILENAME_EXTENSION = ".csv"

BQ_DATASET_ID = "dbt_vfinta"
BQ_TABLE_ID = "source_infokuben"


def http_to_gcs(**context):
    gcs_file_name = f"{GCS_FILENAME_PREFIX}_{context['logical_date'].strftime('%Y_%m_%d')}{GCS_FILENAME_EXTENSION}"
    gcs_blob_path = f"{GCS_PREFIX}/{gcs_file_name}"

    print("Fetching file...")
    response = requests.get(
        INFOKUBEN_URL,
        auth=(INFOKUBEN_USERNAME, INFOKUBEN_PASSWORD),
        timeout=30,
        verify=False # ITs file servers has no SSL certificate :(
    )
    response.raise_for_status()
    data = response.content
    print(f"Downloaded bytes: {len(data)}")

    print("Uploading to GCS...")
    client = storage.Client.from_service_account_info(service_account_info)
    client.bucket(GCS_BUCKET).blob(gcs_blob_path).upload_from_string(
        data, content_type="application/xml", timeout=60
    )
    print(f"Uploaded to gs://{GCS_BUCKET}/{gcs_blob_path}")


def load_to_bq(**context):
    gcs_file_name = f"{GCS_FILENAME_PREFIX}_{context['logical_date'].strftime('%Y_%m_%d')}{GCS_FILENAME_EXTENSION}"
    gcs_blob_path = f"{GCS_PREFIX}/{gcs_file_name}"
    gcs_uri = f"gs://{GCS_BUCKET}/{gcs_blob_path}"

    bq_client = bigquery.Client.from_service_account_info(service_account_info)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        null_marker="NA",
        quote_character="",
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        max_bad_records=0,
    )

    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
    print(f"Loading from: {gcs_uri}")
    job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {BQ_DATASET_ID}.{BQ_TABLE_ID}")


with DAG(
    dag_id="infokuben_to_gcs_bq",
    start_date=datetime(2026, 3, 17),
    schedule="0 7 * * *",
    catchup=False,
) as dag:

    task_http_to_gcs = PythonOperator(
        task_id="http_to_gcs",
        python_callable=http_to_gcs,
    )

    task_load_bq = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
    )

    task_http_to_gcs >> task_load_bq
