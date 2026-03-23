from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
from google.cloud import bigquery

# GCP config
service_account_info = json.loads(os.environ["GCP_SA_KEY"])

GCS_BUCKET = "ur-pipeline-testing"
GCS_PREFIX = "raw/louise"

BQ_DATASET_ID = "dbt_vfinta"
BQ_TABLE_ID = "source_louise_tv"


def load_to_bq(**context):
    bq_client = bigquery.Client.from_service_account_info(service_account_info)
    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)

    uris = [f"gs://{GCS_BUCKET}/{GCS_PREFIX}/*"]

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    print(f"Loading all files from gs://{GCS_BUCKET}/{GCS_PREFIX}/ into {BQ_DATASET_ID}.{BQ_TABLE_ID}")
    job = bq_client.load_table_from_uri(uris, table_ref, job_config=job_config)
    job.result()
    print(f"Done — {job.output_rows} rows loaded")


with DAG(
    dag_id="louise_gcs_to_bq",
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
    )
