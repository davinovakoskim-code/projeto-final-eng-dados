"""
Exporta os data marts agregados da Gold (Delta Lake / MinIO) para o schema
``gold_analytics`` do PostgreSQL, tornando-os acessiveis ao Metabase.

Cada mart e gravado em modo overwrite via JDBC (PostgreSQL JDBC driver ja
incluido nas dependencias do Spark). O schema ``gold_analytics`` e criado
automaticamente caso nao exista (CASCADE via JDBC createTableOptions).

Marts exportados (producao de gold_agregados.py):
    - agg_streamer_visao_geral
    - agg_receita_mensal
    - agg_jogo_popularidade
    - agg_plataforma_resumo

Variaveis de ambiente obrigatorias (herda do .env via Airflow):
    MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

Dentro do Docker o host correto e ``postgres_origem``; o .env define
``POSTGRES_HOST=localhost`` (dev local). A task no Airflow sobrescreve via env.

Uso:
    python src/06_dashboard/gold_to_postgres.py
    python src/06_dashboard/gold_to_postgres.py --marts agg_receita_mensal
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.spark_config import build_spark_session  # noqa: E402

GOLD_SCHEMA = "gold_analytics"

ALL_MARTS = (
    "agg_streamer_visao_geral",
    "agg_receita_mensal",
    "agg_jogo_popularidade",
    "agg_plataforma_resumo",
)


@dataclass(frozen=True)
class ExportConfig:
    gold_bucket: str
    jdbc_url: str
    jdbc_user: str
    jdbc_password: str

    @classmethod
    def from_env(cls) -> "ExportConfig":
        _required_env("MINIO_ROOT_USER")
        _required_env("MINIO_ROOT_PASSWORD")

        host = os.getenv("POSTGRES_HOST", "postgres_origem")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "origem")
        user = _required_env("POSTGRES_USER")
        password = _required_env("POSTGRES_PASSWORD")

        jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

        return cls(
            gold_bucket=os.getenv("MINIO_GOLD_BUCKET", "gold"),
            jdbc_url=jdbc_url,
            jdbc_user=user,
            jdbc_password=password,
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


def read_mart(spark, config: ExportConfig, mart: str):
    path = f"s3a://{config.gold_bucket}/{mart}"
    return spark.read.format("delta").load(path)


def write_to_postgres(df, config: ExportConfig, mart: str) -> int:
    target = f"{GOLD_SCHEMA}.{mart}"
    (
        df.write
        .format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", target)
        .option("user", config.jdbc_user)
        .option("password", config.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .option("createTableOptions", f"TABLESPACE pg_default")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    return df.count()


def ensure_schema(spark, config: ExportConfig) -> None:
    """Cria o schema gold_analytics no Postgres se nao existir."""
    import psycopg2

    host = os.getenv("POSTGRES_HOST", "postgres_origem")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("POSTGRES_DB", "origem"),
        user=config.jdbc_user,
        password=config.jdbc_password,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")
    conn.close()


def run(spark, config: ExportConfig, marts: list[str]) -> None:
    ensure_schema(spark, config)

    for mart in marts:
        print(f"Exportando {mart} ...")
        df = read_mart(spark, config, mart)
        rows = write_to_postgres(df, config, mart)
        print(f"  -> {GOLD_SCHEMA}.{mart}: {rows} linhas")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Le os marts agregados da Gold (Delta/MinIO) e grava no schema "
            f"'{GOLD_SCHEMA}' do PostgreSQL para consumo pelo Metabase."
        )
    )
    parser.add_argument(
        "--marts",
        nargs="*",
        default=[],
        help=(
            "Marts a exportar (separados por espaco ou virgula). "
            "Por padrao exporta todos. Conhecidos: " + ", ".join(ALL_MARTS) + "."
        ),
    )
    return parser


def main() -> None:
    load_environment()
    args = build_parser().parse_args()

    selected = args.marts or list(ALL_MARTS)
    unknown = [m for m in selected if m not in ALL_MARTS]
    if unknown:
        raise SystemExit(
            f"Mart(s) desconhecido(s): {', '.join(unknown)}. "
            f"Conhecidos: {', '.join(ALL_MARTS)}."
        )

    config = ExportConfig.from_env()

    print(f"Gold bucket : s3a://{config.gold_bucket}/")
    print(f"Destino JDBC: {config.jdbc_url} (schema {GOLD_SCHEMA})")
    print(f"Marts       : {len(selected)}\n")

    spark = build_spark_session(app_name="gold-to-postgres")
    try:
        run(spark, config, selected)
    finally:
        spark.stop()

    print(f"\nExport concluido. {len(selected)} mart(s) no schema {GOLD_SCHEMA}.")


if __name__ == "__main__":
    main()
