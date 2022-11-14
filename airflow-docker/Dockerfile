# Base Image
FROM apache/airflow:2.4.2-python3.8
LABEL maintainer="MarcLamberti"

USER root

# Install dependencies and tools
RUN apt-get update && apt-get install -y \
    && apt-get install libreoffice -y 

USER airflow
RUN pip install --upgrade pip && \
    pip install xlrd
