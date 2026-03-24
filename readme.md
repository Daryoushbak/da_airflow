# Airflow Project

This project contains the Airflow DAGs for our analytics team.

## Getting Started

0. You need to receive the .env file which contains all the secrets set as environment variables needed to run the project. Look in the file and updates the neccesary fields and save it in the root of the project.
1. make sure you update the values for:
    - **GOOGLE_APPLICATION_CREDENTIALS** get a serviceaccount json key that has access to GCS and BQ and set the path to it here.
    - *AIRFLOW_HOME* Set it to the root of this repository. you can check with the command `$pwd`
2. create & start your virtual environment (use python 3.13): `python -m venv .venv` (using uv: `uv venv .venv --python 3.13`)
3. To set your environment variables and start your virtual environment run:
```
source .env 
source .venv/bin/activate
If getting error on windows : activate then run these two commands:
PS C:\Users\daba\Documents\valle_airflow_demo> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
PS C:\Users\daba\Documents\valle_airflow_demo> .venv\Scripts\activate

4. Install the dependencies: `pip install -r requirements.txt`
    - If you don’t have pip (or pip3 installed) installed the do this first: `python -m ensurepip --upgrade`
5. run it in local development mode with `airflow standalone`
6. You can start developing your dags. If you change the code inside a dag you might need to click "reparse dag" for it to update.

If you're on mac you might also need to set this environment variable:
export NO_PROXY="*"
As the mac proxy and airflow have some strange interactions and can cause requests to hang indefinitely