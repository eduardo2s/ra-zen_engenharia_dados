# Base Image
FROM apache/airflow:2.4.2-python3.8
LABEL maintainer="MarcLamberti"

USER root

# Install dependencies and tools
RUN apt-get update && apt-get install -y \
    && apt-get install libreoffice -y \
    && apt-get install -y locales \
    && localedef -i pt_BR -f UTF-8 pt_BR.UTF-8 

USER airflow
RUN pip install --upgrade pip && \
    pip install xlrd