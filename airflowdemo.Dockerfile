FROM apache/airflow:2.8.1
# copies the requirements.txt into the current directory in the container we're building
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt