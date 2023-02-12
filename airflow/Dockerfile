FROM apache/airflow:latest-python3.8

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID