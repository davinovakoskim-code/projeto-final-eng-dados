"""
DAG Airflow -- Pipeline da camada Gold (Star Schema -> Agregados).

Encadeia os dois estagios da Gold do data lake medalhao:

    silver_to_gold  ->  gold_agregados

  1) silver_to_gold : le a Silver e materializa o star schema (5 dimensoes + 4 fatos).
  2) gold_agregados : le o star schema e materializa os data marts agregados.

Onde o Spark roda?
    A imagem do Airflow (``docker/airflow/Dockerfile``) ja traz Java + PySpark +
    Delta, e o projeto e montado em ``/opt/project/src`` (com ``PYTHONPATH`` apontando
    para la -- ver ``docker/airflow/docker-compose.yml``). Cada task executa o script
    PySpark correspondente DENTRO do proprio container do Airflow -- o mesmo comando
    usado na execucao manual. As credenciais do MinIO vem do ``.env`` (``env_file``),
    entao ``build_spark_session`` (em ``utils.spark_config``) as encontra no ambiente.

Pre-requisitos:
    - Silver populada (script/DAG anterior: bronze_to_silver).
    - Airflow buildado pela imagem custom (Java + PySpark + Delta) e na rede
      ``datalake`` para alcancar o MinIO em ``minio:9000``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Raiz do projeto montada dentro do container do Airflow (ver docker-compose.yml).
PROJECT_SRC = "/opt/project/src"
GOLD_DIR = f"{PROJECT_SRC}/04_modelagem_gold"


def run_script(script: str, *args: str) -> str:
    """Monta o comando que executa um script PySpark dentro do container do Airflow.

    Usa ``set -euo pipefail`` para que a task falhe (e tente retry) caso o script
    retorne codigo de saida != 0.
    """
    cmd = f"python {GOLD_DIR}/{script}"
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
        bash_command=run_script("silver_to_gold.py"),
        doc_md="Le a Silver e grava o star schema (5 dimensoes + 4 fatos) em Delta na Gold.",
    )

    # Estagio 2: data marts (joins + agregacoes do modelo).
    gold_agregados = BashOperator(
        task_id="gold_agregados",
        bash_command=run_script("gold_agregados.py"),
        doc_md="Le o star schema e grava os data marts agregados (agg_*) em Delta na Gold.",
    )

    silver_to_gold >> gold_agregados
