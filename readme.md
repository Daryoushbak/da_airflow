# Airflow Project

This project contains the Airflow DAGs for our analytics team.

## Getting Started

0.  Set ALL environment variables beforehandin your venv for authentication etc. + add a .json file with the google json at crentials/gcp-service-account.json
1.  Create a virtual environment: `python -m venv .venv` (use python 3.13)
2.  Activate the virtual environment: `source .venv/bin/activate`
3.  install pip `python -m ensurepip --upgrade`
3.  Install the dependencies: `pip install -r requirements.txt`
5.  Set the AIRFLOW_HOME environment variable: `export AIRFLOW_HOME=$(pwd)`
6.  run it in local development mode with `airflow standalone`
