import requests
from typing import Dict
from mage_ai.data_preparation.shared.secrets import get_secret_value


@data_loader
def fetch_spotify_token(*args, **kwargs):

    # Spotify token endpoint URL
    client_id: str = None
    client_secret: str = None
    token_url: str = "https://accounts.spotify.com/api/token"

    # Prepare the data payload for the POST request
    data: Dict[str, str] = {
        "grant_type": "client_credentials",
    }

    # Prepare the headers with Basic Auth encoded credentials
    #auth_header: str = requests.auth._basic_auth_str(client_id, client_secret)
    auth_header = get_secret_value('spotify-Authorization')
    headers: Dict[str, str] = {
        "Authorization": auth_header,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Send POST request to obtain access token
    response: requests.Response = requests.post(token_url, headers=headers, data=data)

    # Raise exception if request failed
    response.raise_for_status()

    # Parse the JSON response to extract the access token
    token_response: Dict = response.json()
    access_token: str = token_response.get("access_token", "")

    return access_token




import gzip
import requests
import io
import json
import pandas as pd

@data_loader
def get_spotify_data(access_token: str, *args, **kwargs):

    url = "https://generic.wg.spotify.com/podcasters-analytics-api/licensors/2KZkYNPuXNo9di6P6LfkuN/batchEpisodesData/2026/03/01"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    response = requests.get(url, headers=headers)
    
    print("Status:", response.status_code)
    print("Bytes received:", len(response.content))

    # We must convert our raw bytes as Mage serializes data between blocks, and raw bytes don't survive that serialization !!!!!!!!!!
    raw_bytes_gzip_json = response.content
    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes_gzip_json)) as f:
        data = f.read().decode("utf-8")

    print("decompression completed")

    return data






from google.cloud import storage
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
import gzip


@data_exporter
def write_dataframe_to_gcs(data, *args, **kwargs):

    # Re-compress back to gzip
    compressed = gzip.compress(data.encode("utf-8"))

    # Initialize GCS client
    service_account_info = json.loads(get_secret_value('GCP_CREDENTIALS_JSON'))
    client = storage.Client.from_service_account_info(service_account_info)
    bucket = client.bucket("ur-pipeline-testing")
    blob = bucket.blob("raw/spotify/spotify_20260301.json.gz")

    blob.upload_from_string(compressed, content_type="application/gzip")

    gcs_uri = f"gs://{bucket}/{blob}"

    print(f"File uploaded to {gcs_uri}")
    
    return gcs_uri



from google.cloud.exceptions import NotFound
from google.cloud import storage, bigquery
import pandas as pd
import json
from mage_ai.data_preparation.shared.secrets import get_secret_value


@data_exporter
def main(gcs_uri: str, *args, **kwargs):
    """
    Load data from a GCS file into a BigQuery table.
    Creates the table if it does not exist, inferring schema from the header.
    """
    print("gcs_uri:", gcs_uri)
    project_id: str = "ur-play-analytics"
    dataset_name: str = "dbt_vfinta"
    table_name: str = "source_spotify_batchepisodesdata"
    table_reference: str = f"{project_id}.{dataset_name}.{table_name}"

    # Initialize clients
    service_account_info = json.loads(get_secret_value('GCP_CREDENTIALS_JSON'))
    bq_client = bigquery.Client.from_service_account_info(service_account_info)
    gcs_client = storage.Client.from_service_account_info(service_account_info)

    """
    # Download the file from GCS
    bucket = gcs_client.bucket("ur-pipeline-testing")
    blob = bucket.blob("raw/spotify/spotify_20260301.json.gz")
    data_bytes = blob.download_as_bytes()

    #checking what dates we have in BQ
    query = f"SELECT DISTINCT date FROM `{table_reference}`"
    bq_dates = {row.date.strftime("%Y%m%d") for row in bq_client.query(query)}

    #checking what dates we have in GCS
    blobs = bucket.list_blobs(prefix="raw/spotify/")
    gcs_dates = set()
    for blob in blobs:
        # Extract date from "raw/spotify/spotify_20260301.json.gz"
        filename = blob.name.split("/")[-1]  # spotify_20260301.json.gz
        date_str = filename.replace("spotify_", "").replace(".json.gz", "")  # 20260301
        gcs_dates.add(date_str)

    dates_to_load = gcs_dates - bq_dates  # dates in GCS but not in BQ
    """
    

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    #for date_str in sorted(dates_to_load):
    for date_str in ["20260301"]:
        job = bq_client.load_table_from_uri(f"gs://ur-pipeline-testing/raw/spotify/spotify_{date_str}.json.gz", table_reference, job_config=job_config)
        job.result()
        #print(f"Loaded {job.output_rows} rows from filename ({date_str}) into {table_ref_str}")

    
    
