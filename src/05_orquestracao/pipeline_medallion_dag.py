from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

_INGESTAO = "/opt/project/src/02_ingestao/ingestao_postgres.py"
_LANDING_TO_BRONZE = "/opt/project/src/03_transformacao/landing_to_bronze.py"

# Todas as tabelas do banco de origem (schema.sql).
_TABLES = (
    "plataformas jogos streamers viewers emotes transmissoes "
    "visualizacoes follows assinaturas doacoes clips raids moderadores"
)

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

    # Extrai todas as tabelas do Postgres de origem e sobe os CSVs para
    # s3a://landing/extraction_date=<hoje>/ no MinIO.
    # POSTGRES_HOST é sobrescrito porque o .env usa "localhost" (dev local),
    # mas dentro do container o Postgres está em "postgres_origem".
    landing = BashOperator(
        task_id="landing",
        bash_command=f"python {_INGESTAO} --upload-minio --tables {_TABLES}",
        # .env usa localhost:5433 (dev local); dentro do Docker é postgres_origem:5432
        env={"POSTGRES_HOST": "postgres_origem", "POSTGRES_PORT": "5432"},
        append_env=True,
    )

    # Lê os CSVs da Landing e grava cada tabela como Delta Lake cru na Bronze.
    # Usa a data de hoje para localizar a partição (mesmo padrão do passo landing).
    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"python {_LANDING_TO_BRONZE}",
    )

    # issue #57: ler Bronze, aplicar Data Quality e gravar Silver
    silver = EmptyOperator(task_id="silver")

    # issue #57: ler Silver e alimentar star schema Gold (Kimball)
    gold = EmptyOperator(task_id="gold")

    fim = EmptyOperator(task_id="fim")

    inicio >> landing >> bronze >> silver >> gold >> fim
