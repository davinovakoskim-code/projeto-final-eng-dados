FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    minio==7.2.7 \
    psycopg2-binary==2.9.9 \
    python-dotenv
