# Airflow Project

This project contains the Airflow DAGs for our analytics team.

## Getting Started

0.  Set ALL environment variables beforehandin your venv for authentication etc. + add a .json file with the google json at crentials/gcp-service-account.json
export GOOGLE_APPLICATION_CREDENTIALS=X
export AIRFLOW_HOME=X
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080 airflow standalone
export AIRFLOW_CONN_MMS_SFTP_HOST=X
export AIRFLOW_CONN_MMS_SFTP_USERNAME=X
export AIRFLOW_CONN_MMS_SFTP_PASSWORD=X
export AIRFLOW_CONN_MMS_REMOTE_FILE_PATH=X
export AIRFLOW_CONN_MMS_SFTP_PORT=X
1.  Create a virtual environment: `python -m venv .venv` (use python 3.13)
2.  Activate the virtual environment: `source .venv/bin/activate`
3.  install pip `python -m ensurepip --upgrade`
4.  Install the dependencies: `pip install -r requirements.txt`
5.  Set the AIRFLOW_HOME environment variable: `export AIRFLOW_HOME=$(pwd)`
6.  run it in local development mode with `airflow standalone`
