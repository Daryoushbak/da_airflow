from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import io
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

FILE_NAME = f"{GCS_FILENAME_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{GCS_FILENAME_EXTENSION}"

def http_to_gcs(**context):
    """Fetch file from HTTP endpoint and upload to GCS, overwriting the previous."""
    print("INFO HERE!!!!")
    response = requests.get(
        INFOKUBEN_URL,
        auth=(INFOKUBEN_USERNAME, INFOKUBEN_PASSWORD),
        verify=True,
        timeout=120
    )
    print("the get resquest was sent")
    response.raise_for_status()
    print("raised")
    data = response.content
    print("data was saved!")

    print(f"Fetched {len(data)} bytes from {INFOKUBEN_URL}, assigning file_name: {FILE_NAME}")

    gcs_blob_path = f"{GCS_PREFIX}/{FILE_NAME}"
    gcs_client = storage.Client.from_service_account_info(service_account_info)
    blob = gcs_client.bucket(GCS_BUCKET).blob(gcs_blob_path)
    blob.upload_from_file(io.BytesIO(data))
    print(f"Uploaded to gs://{GCS_BUCKET}/{gcs_blob_path}")


def load_to_bq(**context):
    """Truncate and reload the BQ table from the latest GCS file."""
    bq_client = bigquery.Client.from_service_account_info(service_account_info)
    gcs_uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{FILE_NAME}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        null_marker="NA",
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\;",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        max_bad_records=0,
    )

    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
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
