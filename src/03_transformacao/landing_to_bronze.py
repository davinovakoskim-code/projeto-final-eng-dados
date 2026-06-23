"""
Transformacao Landing -> Bronze.

Le os CSVs brutos da camada Landing no MinIO (gerados pela ingestao em
``src/02_ingestao/ingestao_postgres.py``) e os grava como tabelas Delta Lake na
camada Bronze. Os dados sao mantidos FIEIS A ORIGEM: nenhuma regra de negocio e
aplicada nesta etapa. Todas as colunas sao lidas como string (sem inferencia de
tipo) e apenas colunas de auditoria (proveniencia) sao adicionadas. A tipagem e a
limpeza ficam para a camada Silver.

Layout de origem (Landing):
    s3a://landing/extraction_date=YYYY-MM-DD/public__<tabela>.csv

Layout de destino (Bronze): uma tabela Delta por tabela de origem.
    s3a://bronze/<tabela>/

Uso:
    python src/03_transformacao/landing_to_bronze.py --extraction-date 2026-06-22
"""

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

# Garante que src/ esteja no path para importar utils.spark_config, tanto local
# quanto dentro do container Jupyter (/home/jovyan/work/src).
_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.spark_config import build_spark_session  # noqa: E402


DEFAULT_LANDING_BUCKET = "landing"
DEFAULT_BRONZE_BUCKET = "bronze"


@dataclass(frozen=True)
class LandingConfig:
    """Configuracao de acesso ao MinIO e dos buckets origem/destino."""

    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    landing_bucket: str
    bronze_bucket: str

    @classmethod
    def from_env(cls) -> "LandingConfig":
        endpoint = _required_env("MINIO_ENDPOINT")
        if "://" in endpoint:
            endpoint = endpoint.split("://", 1)[1]

        return cls(
            endpoint=endpoint,
            access_key=_required_env("MINIO_ROOT_USER"),
            secret_key=_required_env("MINIO_ROOT_PASSWORD"),
            secure=_env_bool("MINIO_SECURE", False),
            landing_bucket=os.getenv("MINIO_LANDING_BUCKET", DEFAULT_LANDING_BUCKET),
            bronze_bucket=os.getenv("MINIO_BRONZE_BUCKET", DEFAULT_BRONZE_BUCKET),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y"}


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


def build_minio_client(config: LandingConfig):
    from minio import Minio

    return Minio(
        config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
    )


def table_name_from_object(object_name: str) -> str:
    """Deriva o nome da tabela Delta a partir do nome do objeto na Landing.

    Ex.: 'extraction_date=2026-06-22/public__streamers.csv' -> 'streamers'.
    O prefixo de schema ('public__') e o sufixo '.csv' sao removidos.
    """
    file_name = object_name.rsplit("/", 1)[-1]
    if file_name.endswith(".csv"):
        file_name = file_name[: -len(".csv")]
    return file_name.split("__")[-1]


def list_landing_objects(
    config: LandingConfig,
    extraction_date: date,
) -> list[str]:
    """Lista os CSVs da particao de uma data de extracao na Landing."""
    client = build_minio_client(config)
    prefix = f"extraction_date={extraction_date.isoformat()}/"

    objects = client.list_objects(
        config.landing_bucket, prefix=prefix, recursive=True
    )
    return [
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(".csv")
    ]


def filter_objects_by_tables(
    object_names: Sequence[str],
    tables: Sequence[str],
) -> list[str]:
    """Mantem apenas os objetos cujas tabelas derivadas estao em ``tables``."""
    wanted = set(tables)
    return [
        name for name in object_names if table_name_from_object(name) in wanted
    ]


def parse_table_names(values) -> list[str]:
    table_names: list[str] = []
    for value in values:
        for table_name in value.split(","):
            table_name = table_name.strip()
            if table_name:
                table_names.append(table_name)
    return table_names


def transform_object_to_bronze(
    spark,
    config: LandingConfig,
    object_name: str,
    extraction_date: date,
) -> tuple[str, int]:
    """Le um CSV da Landing e grava a tabela Delta correspondente na Bronze.

    Retorna (nome_da_tabela, quantidade_de_registros).
    """
    from pyspark.sql import functions as F

    table = table_name_from_object(object_name)
    landing_path = f"s3a://{config.landing_bucket}/{object_name}"
    bronze_path = f"s3a://{config.bronze_bucket}/{table}"

    # Leitura fiel a origem: header como nomes de coluna, tudo como string.
    df = (
        spark.read
        .option("header", "true")
        .csv(landing_path)
    )

    # Colunas de auditoria (proveniencia) — nao sao regra de negocio.
    df = (
        df
        .withColumn("_extraction_date", F.lit(extraction_date.isoformat()))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

    count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )

    return table, count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Le os CSVs da camada Landing no MinIO e grava como Delta Lake na "
            "camada Bronze, mantendo os dados fieis a origem."
        ),
    )
    parser.add_argument(
        "--extraction-date",
        type=date.fromisoformat,
        default=date.today(),
        help=(
            "Data da extracao (YYYY-MM-DD) usada para localizar a particao na "
            "Landing. Padrao: hoje."
        ),
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=[],
        help=(
            "Tabelas a processar (separadas por espaco ou virgula). Por padrao, "
            "processa todos os CSVs encontrados na particao da Landing."
        ),
    )
    parser.add_argument(
        "--landing-bucket",
        help="Sobrescreve o bucket de origem (padrao: MINIO_LANDING_BUCKET ou 'landing').",
    )
    parser.add_argument(
        "--bronze-bucket",
        help="Sobrescreve o bucket de destino (padrao: MINIO_BRONZE_BUCKET ou 'bronze').",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    config = LandingConfig.from_env()

    if args.landing_bucket:
        config = LandingConfig(**{**config.__dict__, "landing_bucket": args.landing_bucket})
    if args.bronze_bucket:
        config = LandingConfig(**{**config.__dict__, "bronze_bucket": args.bronze_bucket})

    extraction_date = args.extraction_date

    object_names = list_landing_objects(config, extraction_date)
    if not object_names:
        raise SystemExit(
            f"Nenhum CSV encontrado em "
            f"s3a://{config.landing_bucket}/extraction_date={extraction_date.isoformat()}/. "
            f"Execute a ingestao antes (src/02_ingestao/ingestao_postgres.py --upload-minio)."
        )

    selected_tables = parse_table_names(args.tables)
    if selected_tables:
        object_names = filter_objects_by_tables(object_names, selected_tables)
        if not object_names:
            raise SystemExit(
                "Nenhuma das tabelas informadas em --tables foi encontrada na "
                f"particao extraction_date={extraction_date.isoformat()}."
            )

    print(
        f"Landing: s3a://{config.landing_bucket}/"
        f"extraction_date={extraction_date.isoformat()}/"
    )
    print(f"Bronze : s3a://{config.bronze_bucket}/<tabela>/")
    print(f"CSVs a processar: {len(object_names)}\n")

    spark = build_spark_session(app_name="landing-to-bronze")
    try:
        for object_name in object_names:
            table, count = transform_object_to_bronze(
                spark, config, object_name, extraction_date
            )
            print(
                f"- {table}: {count} registros gravados em "
                f"s3a://{config.bronze_bucket}/{table}/"
            )
    finally:
        spark.stop()

    print("\nTransformacao Landing -> Bronze concluida.")


if __name__ == "__main__":
    main()
