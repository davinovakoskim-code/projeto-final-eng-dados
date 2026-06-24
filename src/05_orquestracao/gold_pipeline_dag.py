"""
DAG Airflow -- Pipeline da camada Gold (Star Schema -> Agregados).

Encadeia os dois estagios da Gold do data lake medalhao:

    silver_to_gold  ->  gold_agregados

  1) silver_to_gold : le a Silver e materializa o star schema (5 dimensoes + 4 fatos).
  2) gold_agregados : le o star schema e materializa os data marts agregados.

Por que ``docker exec``?
    O Spark (PySpark + Delta + jars + acesso s3a ao MinIO) vive no container
    ``jupyter_spark``, com o projeto montado em ``/home/jovyan/work``. O Airflow
    (imagem propria, sem Spark) apenas ORQUESTRA: cada task dispara o script Python
    correspondente DENTRO do ``jupyter_spark`` via ``docker exec`` -- exatamente o
    mesmo comando documentado para execucao manual. Assim as credenciais do MinIO
    (carregadas do .env no jupyter_spark) ja estao disponiveis no ambiente do exec.

Pre-requisitos:
    - Container ``jupyter_spark`` em execucao (docker/docker-compose.yml).
    - Silver populada (DAG/anterior: bronze_to_silver).
    - Airflow com Docker CLI + acesso ao socket (docker/airflow/docker-compose.yml).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Container onde o Spark roda e caminho do projeto montado dentro dele.
SPARK_CONTAINER = "jupyter_spark"
PROJECT_ROOT = "/home/jovyan/work"
GOLD_DIR = f"{PROJECT_ROOT}/src/04_modelagem_gold"


def spark_exec(script: str, *args: str) -> str:
    """Monta o comando que executa um script PySpark dentro do jupyter_spark.

    Usa ``set -euo pipefail`` para que a task falhe (e tente retry) caso o
    ``docker exec`` ou o proprio script retornem codigo de saida != 0.
    """
    cmd = f"docker exec {SPARK_CONTAINER} python {GOLD_DIR}/{script}"
    if args:
        cmd += " " + " ".join(args)
    return f"set -euo pipefail\n{cmd}"


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="gold_pipeline",
    description="Camada Gold: star schema (silver_to_gold) -> agregados (gold_agregados).",
    default_args=default_args,
    schedule=None,            # disparo manual; encadear apos a Silver quando houver DAG dela.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "kimball", "delta"],
    doc_md=__doc__,
) as dag:

    # Estagio 1: dimensoes + fatos (star schema).
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=spark_exec("silver_to_gold.py"),
        doc_md="Le a Silver e grava o star schema (5 dimensoes + 4 fatos) em Delta na Gold.",
    )

    # Estagio 2: data marts (joins + agregacoes do modelo).
    gold_agregados = BashOperator(
        task_id="gold_agregados",
        bash_command=spark_exec("gold_agregados.py"),
        doc_md="Le o star schema e grava os data marts agregados (agg_*) em Delta na Gold.",
    )

    silver_to_gold >> gold_agregados
