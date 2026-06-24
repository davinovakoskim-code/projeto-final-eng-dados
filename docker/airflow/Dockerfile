# Imagem do Airflow usada para orquestrar a pipeline.
#
# Estende a imagem oficial e instala apenas o Docker CLI: as DAGs disparam os
# jobs PySpark DENTRO do container jupyter_spark via `docker exec` (o Airflow nao
# roda Spark; ele orquestra). O acesso ao Docker e via socket do host, montado no
# docker-compose.yml.
FROM apache/airflow:2.9.3

USER root

# docker.io traz o cliente `docker` (CLI). O daemon nao e usado aqui -- falamos
# com o daemon do host pelo socket montado em /var/run/docker.sock.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
