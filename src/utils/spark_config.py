"""
Fabrica de SparkSession configurada para:
  - Delta Lake 3.2.0 (delta-spark)
  - MinIO via protocolo s3a (hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.367)
  - PostgreSQL via JDBC

Uso:
    from utils.spark_config import build_spark_session

    spark = build_spark_session()
"""

import os

# Coordenadas Maven para Spark 3.5.x / Hadoop 3.3.4
_DELTA_PACKAGE  = "io.delta:delta-spark_2.12:3.2.0"
_HADOOP_AWS     = "org.apache.hadoop:hadoop-aws:3.3.4"
_AWS_SDK_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.367"
_POSTGRES_JDBC  = "org.postgresql:postgresql:42.7.3"

# configure_spark_with_delta_pip substitui spark.jars.packages pelo pacote delta;
# hadoop-aws e aws-sdk-bundle precisam ser passados como extra_packages.
EXTRA_PACKAGES = [_HADOOP_AWS, _AWS_SDK_BUNDLE, _POSTGRES_JDBC]


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def _minio_endpoint() -> str:
    """Retorna o endpoint do MinIO sem esquema. Dentro do Docker usa 'minio:9000'."""
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    if "://" in endpoint:
        endpoint = endpoint.split("://", 1)[1]
    return endpoint


def build_spark_session(app_name: str = "projeto-eng-dados"):
    """
    Constroi e retorna uma SparkSession pronta para ler/escrever no MinIO via s3a
    e no Delta Lake.

    Os JARs sao baixados automaticamente via spark.jars.packages na primeira execucao
    (cached em ~/.ivy2 dentro do container).
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    access_key = _required_env("MINIO_ROOT_USER")
    secret_key  = _required_env("MINIO_ROOT_PASSWORD")
    endpoint    = _minio_endpoint()

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake extensions (também definidas por configure_spark_with_delta_pip)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # s3a / MinIO
        .config("spark.hadoop.fs.s3a.endpoint",           f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",         access_key)
        .config("spark.hadoop.fs.s3a.secret.key",         secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # MinIO usa HTTP simples no Docker
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Evita 200 partitions de shuffle em ambiente local/dev
        .config("spark.sql.shuffle.partitions", "4")
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=EXTRA_PACKAGES
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
