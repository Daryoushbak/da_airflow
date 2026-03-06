from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import paramiko
import os
from google.cloud import bigquery, storage

# SFTP config
SFTP_HOST = os.getenv("AIRFLOW_CONN_MMS_SFTP_HOST")
SFTP_USERNAME = os.getenv("AIRFLOW_CONN_MMS_SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("AIRFLOW_CONN_MMS_SFTP_PASSWORD")
SFTP_PORT = os.getenv("AIRFLOW_CONN_MMS_SFTP_PORT")
REMOTE_PATH = "/census/prod/"

# GCS config
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-gcs-bucket")
GCS_PREFIX = "mms_av"

# BigQuery config
BQ_DATASET_ID = "dbt_vfinta"
BQ_TABLE_ID = "source_mms_av"
BQ_DATE_COLUMN = "file_date"  # column used for deduplication; adjust if differs


def sftp_to_gcs(ds, **context):
    """Stream file directly from SFTP to GCS for the logical date (ds = YYYY-MM-DD)."""
    
    # logical_date is a pendulum.DateTime object auto-populated by Airflow, representing the execution date for this run
    execution_date = logical_date.strftime("%Y%m%d")
    print(execution_date, "should match the YYYYMMDD prefix of the SFTP file")

    #date_str = ds.replace("-", "")  # YYYYMMDD

    transport = paramiko.Transport((SFTP_HOST, int(SFTP_PORT)))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(
        transport, window_size=2**31 - 1, max_packet_size=2**31 - 1
    )

    try:
        remote_filename = next(
            (f for f in sftp.listdir(REMOTE_PATH) if f.startswith(execution_date)), None
        )
        if not remote_filename:
            raise FileNotFoundError(f"No SFTP file found for date {execution_date}")

        gcs_blob_path = f"{GCS_PREFIX}/{remote_filename}"
        blob = storage.Client().bucket(GCS_BUCKET).blob(gcs_blob_path)

        with sftp.open(os.path.join(REMOTE_PATH, remote_filename), "rb") as f:
            f.prefetch()  # buffers ahead for faster reads over SFTP
            blob.upload_from_file(f)

        print(f"Streamed {remote_filename} to gs://{GCS_BUCKET}/{gcs_blob_path}")
    finally:
        sftp.close()
        transport.close()


def load_to_bq(**context):
    """Compare dates in GCS vs BQ and load any files not yet present in the table."""
    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    table_ref_str = f"{BQ_DATASET_ID}.{BQ_TABLE_ID}"

    # Dates already in BQ
    try:
        rows = bq_client.query(
            f"SELECT DISTINCT {BQ_DATE_COLUMN} FROM `{table_ref_str}`"
        ).result()
        bq_dates = {row[0] for row in rows}
    except Exception:
        bq_dates = set()  # table doesn't exist yet on first run

    # Dates available in GCS (derived from the YYYYMMDD filename prefix)
    blobs = gcs_client.list_blobs(GCS_BUCKET, prefix=f"{GCS_PREFIX}/")
    gcs_files = {
        blob.name.split("/")[-1]: blob.name.split("/")[-1][:8]  # filename -> YYYYMMDD
        for blob in blobs
        if blob.name != f"{GCS_PREFIX}/"
    }
    gcs_dates = set(gcs_files.values())

    dates_to_load = gcs_dates - bq_dates
    if not dates_to_load:
        print("No new dates to load into BQ.")
        return

    print(f"Dates in BQ:  {sorted(bq_dates)}")
    print(f"Dates in GCS: {sorted(gcs_dates)}")
    print(f"Starting Loading of:      {sorted(dates_to_load)}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        quote_character="",
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
    files_to_load = {f: d for f, d in gcs_files.items() if d in dates_to_load}

    for filename, date_str in sorted(files_to_load.items()):
        gcs_uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{filename}"
        job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows from {filename} ({date_str}) into {table_ref_str}")


with DAG(
    dag_id="mms_av_sftp_to_bq",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True, # must be true for backfilling and logical date handling!
) as dag:

    task_sftp_to_gcs = PythonOperator(
        task_id="sftp_to_gcs",
        python_callable=sftp_to_gcs,
    )

    task_load_bq = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
    )

    task_sftp_to_gcs >> task_load_bq
