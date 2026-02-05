
import json
from datetime import datetime
from airflow.decorators import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- CONFIGURATION ---
GCP_PROJECT_ID = "ur-play-analytics"  # TODO: Change this to your GCP Project ID
BIGQUERY_DATASET = "dbt_vfinta"
BIGQUERY_TABLE = "katt_fakta"
BIGQUERY_TABLE_SCHEMA = [
    bigquery.SchemaField("fact", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("length", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
]

@dag(
    dag_id="gcp_pipeline_demo",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo"],
    doc_md="""
    This DAG demonstrates a simple ETL pipeline:
    1.  **Fetch Data**: Fetches a random cat fact from the `catfact.ninja` API.
    2.  **Load Data**: Loads the fact into a BigQuery table. Creates the table if it doesn't exist.
    """,
)
def gcp_pipeline_demo():
    """
    A simple DAG to fetch a cat fact and load it into BigQuery.
    """

    @task
    def fetch_cat_fact():
        """
        Fetches a random cat fact from the API and returns it as a dictionary.
        """
        import requests
        response = requests.get("https://catfact.ninja/fact")
        response.raise_for_status()
        fact_data = response.json()
        fact_data["timestamp"] = datetime.now().isoformat()
        return fact_data

    @task
    def load_to_bq(fact_data: dict):
        """
        Takes the fetched data and loads it into BigQuery.
        Creates the table if it does not exist.
        """
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        # Try to get the table, and if it's not found, create it.
        try:
            client.get_table(table_id)  # Make an API request.
            print(f"Table {table_id} already exists.")
        except NotFound:
            print(f"Table {table_id} not found. Creating table...")
            table = bigquery.Table(table_id, schema=BIGQUERY_TABLE_SCHEMA)
            client.create_table(table)  # Make an API request.
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

        # The data needs to be a list of dictionaries for insert_rows_json
        rows_to_insert = [fact_data]
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            raise Exception(f"Errors occurred while inserting rows into BigQuery: {errors}")
        else:
            print(f"Successfully inserted {len(rows_to_insert)} row(s) into {table_id}")

    # --- Define Task Dependencies ---
    fetched_data = fetch_cat_fact()
    load_to_bq(fetched_data)

# Instantiate the DAG
gcp_pipeline_demo()
