from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import paramiko
import os
from google.cloud import bigquery

SFTP_HOST = os.getenv("AIRFLOW_CONN_MMS_SFTP_HOST")
SFTP_USERNAME = os.getenv("AIRFLOW_CONN_MMS_SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("AIRFLOW_CONN_MMS_SFTP_PASSWORD")
SFTP_PORT = os.getenv("AIRFLOW_CONN_MMS_SFTP_PORT")
REMOTE_PATH = "/census/prod/"
LOCAL_PATH = "source/mms/mms_av"
BQ_DATASET_ID = "your_dataset_id" # Please change this to your BigQuery dataset ID
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
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    filename_to_upload = None
    for filename in os.listdir(LOCAL_PATH):
        if filename.startswith(yesterday_str):
            filename_to_upload = filename
            break

    if not filename_to_upload:
        raise FileNotFoundError(f"No file found for yesterday's date ({yesterday_str}) in {LOCAL_PATH}")

    local_file_path = os.path.join(LOCAL_PATH, filename_to_upload)

    client = bigquery.Client()
    temp_table_id = f"temp_table_{yesterday_str}"
    
    # 1. Overwrite to new temp table
    temp_table_ref = client.dataset(BQ_DATASET_ID).table(temp_table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(local_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, temp_table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into temp table {BQ_DATASET_ID}:{temp_table_id}.")

    # 2. Merge to the existing table
    # PLEASE COMPLETE THIS MERGE QUERY. You need to define the join condition (e.g., ON T.id = S.id)
    # and the update/insert logic based on your data's primary key.
    merge_query = f"""
        MERGE `{BQ_DATASET_ID}.{BQ_TABLE_ID}` T
        USING `{BQ_DATASET_ID}.{temp_table_id}` S
        ON T.id = S.id -- Replace 'id' with your primary key column(s)
        WHEN MATCHED THEN
            UPDATE SET T.data = S.data -- Replace with the columns you want to update
        WHEN NOT MATCHED THEN
            INSERT (id, data) VALUES (S.id, S.data) -- Replace with your column names
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    print(f"Merged data from {temp_table_id} into {BQ_TABLE_ID}.")

    # 3. Delete temp table
    client.delete_table(temp_table_ref)
    print(f"Deleted temp table {BQ_DATASET_ID}:{temp_table_id}.")


with DAG(
    dag_id="sftp_to_bq_merge_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
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