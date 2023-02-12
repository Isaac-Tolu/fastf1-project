FROM apache/airflow:2.5.1

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR ${AIRFLOW_HOME}
USER $AIRFLOW_UID