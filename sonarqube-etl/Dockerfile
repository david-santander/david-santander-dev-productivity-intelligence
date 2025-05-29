FROM apache/airflow:2.7.3-python3.10

USER root

# Install additional system dependencies if needed
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install additional Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs
COPY --chown=airflow:airflow airflow/dags /opt/airflow/dags

# Copy configuration if any
COPY --chown=airflow:airflow airflow/config /opt/airflow/config