from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

_INGESTAO         = "/opt/project/src/02_ingestao/ingestao_postgres.py"
_LANDING_TO_BRONZE = "/opt/project/src/03_transformacao/landing_to_bronze.py"
_BRONZE_TO_SILVER  = "/opt/project/src/03_transformacao/bronze_to_silver.py"
_SILVER_TO_GOLD    = "/opt/project/src/04_modelagem_gold/silver_to_gold.py"
_GOLD_AGREGADOS    = "/opt/project/src/04_modelagem_gold/gold_agregados.py"
_GOLD_TO_POSTGRES  = "/opt/project/src/06_dashboard/gold_to_postgres.py"
_METABASE_SETUP    = "/opt/project/src/06_dashboard/metabase_setup.py"

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

    # Lê Bronze, aplica Data Quality e grava Silver (válidos + quarentena).
    silver = BashOperator(
        task_id="silver",
        bash_command=f"python {_BRONZE_TO_SILVER}",
    )

    # Lê Silver, materializa star schema Kimball na Gold (dims + fatos).
    gold_star = BashOperator(
        task_id="gold_star",
        bash_command=f"python {_SILVER_TO_GOLD}",
    )

    # Lê star schema Gold e materializa data marts agregados.
    gold_marts = BashOperator(
        task_id="gold_marts",
        bash_command=f"python {_GOLD_AGREGADOS}",
    )

    # Exporta os marts agregados da Gold para o schema gold_analytics no Postgres
    # para consumo pelo Metabase.
    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=f"python {_GOLD_TO_POSTGRES}",
        env={"POSTGRES_HOST": "postgres_origem", "POSTGRES_PORT": "5432"},
        append_env=True,
    )

    # Cria/atualiza a conexao, os cards e o dashboard no Metabase.
    # Dentro do Docker, o Airflow acessa o Metabase pelo nome do servico.
    metabase_setup = BashOperator(
        task_id="metabase_setup",
        bash_command=f"python {_METABASE_SETUP} --metabase-url http://metabase:3000",
        env={
            "METABASE_URL": "http://metabase:3000",
            "POSTGRES_HOST": "postgres_origem",
            "POSTGRES_PORT": "5432",
        },
        append_env=True,
    )

    fim = EmptyOperator(task_id="fim")

    (
        inicio
        >> landing
        >> bronze
        >> silver
        >> gold_star
        >> gold_marts
        >> gold_to_postgres
        >> metabase_setup
        >> fim
    )
