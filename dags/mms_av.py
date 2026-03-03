from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import paramiko
import os
from google.cloud import bigquery

#DARYOUSH EDITTT!!!!

# connection to source SFTP server and BigQuery configuration
SFTP_HOST = os.getenv("AIRFLOW_CONN_MMS_SFTP_HOST")
SFTP_USERNAME = os.getenv("AIRFLOW_CONN_MMS_SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("AIRFLOW_CONN_MMS_SFTP_PASSWORD")
SFTP_PORT = os.getenv("AIRFLOW_CONN_MMS_SFTP_PORT")
REMOTE_PATH = "/census/prod/"
LOCAL_PATH = "source/mms/mms_av"
BQ_DATASET_ID = "dbt_vfinta"
BQ_TABLE_ID = "source_mms_av"

def download_yesterdays_file():
    os.makedirs(LOCAL_PATH, exist_ok=True)
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    transport = paramiko.Transport((SFTP_HOST, int(SFTP_PORT)))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(transport, window_size=2**31-1, max_packet_size=2**31-1)
    
    remote_filename = None
    for filename in sftp.listdir(REMOTE_PATH):
        if filename.startswith(yesterday_str):
            remote_filename = filename
            break
    
    if not remote_filename:
        print(f"No file found on SFTP server for yesterday's date ({yesterday_str}).")
        sftp.close()
        transport.close()
        return

    print(f"Downloading {remote_filename}...")
    remote_file_path = os.path.join(REMOTE_PATH, remote_filename)
    local_file_path = os.path.join(LOCAL_PATH, remote_filename)
    sftp.get(remote_file_path, local_file_path)
    print(f"Downloaded {remote_filename} to {local_file_path}")

    sftp.close()
    transport.close()

def upload_and_merge_to_bq():

    client = bigquery.Client()
    """
    query = f"SELECT DISTINCT file_date FROM `{BQ_DATASET_ID}.{BQ_TABLE_ID}`"
    query_job = client.query(query)
    bq_dates = {row.file_date for row in query_job}
    print(f"Dates already in BigQuery: {bq_dates}")
    
    local_files = {}
    for filename in os.listdir(LOCAL_PATH):
        if filename.endswith(".tsv.gz"):
            date_str = filename.split('_')[0]
            datetime.strptime(date_str, '%Y%m%d')
            local_files[date_str] = filename

    print(f"Local files found: {local_files}")

    #Determine which files to upload
    files_to_upload = {date: filename for date, filename in local_files.items() if date not in bq_dates}
    print(f"Files to upload: {files_to_upload}")

    if not files_to_upload:
        print("No new files to upload.")
        return
    """
    # 4. Append new data to BigQuery
    table_ref = client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        quote_character='',  # disables quote character interpretation
        source_format=bigquery.SourceFormat.CSV, # CSV format can handle TSV with delimiter
        field_delimiter='\t', # Added for TSV files
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )
    
    """
    for date, filename in files_to_upload.items():
        local_file_path = os.path.join(LOCAL_PATH, filename)
        print(f"Uploading {filename}...")
        with open(local_file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows from {filename} into {BQ_DATASET_ID}.{BQ_TABLE_ID}.")
    """
    filename = "20260302-UR-content_AV_Prod.tsv.gz"
    local_file_path = os.path.join(LOCAL_PATH, filename)
    print(f"Uploading {filename}...")
    with open(local_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows from {filename} into {BQ_DATASET_ID}.{BQ_TABLE_ID}.")


with DAG(
    dag_id="sftp_to_bq_merge_dag",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    task_download_file = PythonOperator(
        task_id="download_yesterdays_file",
        python_callable=download_yesterdays_file
    )

    task_upload_and_merge = PythonOperator(
        task_id="upload_and_merge_to_bq",
        python_callable=upload_and_merge_to_bq
    )

    task_download_file >> task_upload_and_merge