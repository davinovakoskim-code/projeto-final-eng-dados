from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "eng-dados",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_medallion",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "eng-dados"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # issue #56: extrair tabelas do Postgres de origem → Landing (CSV no MinIO)
    landing = EmptyOperator(task_id="landing")

    # issue #56: ler Landing e gravar em Delta Lake cru (camada Bronze)
    bronze = EmptyOperator(task_id="bronze")

    # issue #57: ler Bronze, aplicar Data Quality e gravar Silver
    silver = EmptyOperator(task_id="silver")

    # issue #57: ler Silver e alimentar star schema Gold (Kimball)
    gold = EmptyOperator(task_id="gold")

    fim = EmptyOperator(task_id="fim")

    inicio >> landing >> bronze >> silver >> gold >> fim
