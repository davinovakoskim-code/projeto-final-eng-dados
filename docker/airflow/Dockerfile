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
    python-dotenv \
    requests==2.34.2

# Pré-aquece o cache Ivy2 durante o build para que os JARs do Spark
# (delta-spark, hadoop-aws, aws-sdk-bundle) não precisem ser baixados
# do Maven a cada execução de task no Airflow.
RUN python -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
spark = configure_spark_with_delta_pip( \
    SparkSession.builder.master('local'), \
    extra_packages=[ \
        'org.apache.hadoop:hadoop-aws:3.3.4', \
        'com.amazonaws:aws-java-sdk-bundle:1.12.367', \
    ] \
).getOrCreate(); \
spark.stop() \
"
