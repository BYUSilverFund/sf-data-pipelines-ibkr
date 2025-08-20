# Multi-stage build for efficiency
FROM apache/airflow:3.0.4 as base

# Install dependencies in separate layer
COPY requirements.txt .
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Final stage
FROM base as final
COPY dags/ /opt/airflow/dags/
COPY config/ /opt/airflow/config/
