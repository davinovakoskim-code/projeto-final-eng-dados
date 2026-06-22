import argparse
import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from minio_client import (
    LandingPathError,
    MinioConfig,
    MinioConfigError,
    MinioConnectionError,
    build_minio_client,
    upload_csv_to_landing,
    validate_bucket,
)


DEFAULT_SCHEMA = "public"


@dataclass(frozen=True)
class ExtractedTable:
    schema_name: str
    table_name: str
    columns: list[str]
    rows: list[tuple]

    @property
    def name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class TableNotFoundError(RuntimeError):
    pass


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=_env_int("POSTGRES_PORT", 5432),
            user=_required_env("POSTGRES_USER"),
            password=_required_env("POSTGRES_PASSWORD"),
            database=_required_env("POSTGRES_DB"),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Variavel de ambiente {name} deve ser um numero inteiro."
        ) from exc


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


def parse_table_names(values: Iterable[str]) -> list[str]:
    table_names: list[str] = []

    for value in values:
        for table_name in value.split(","):
            table_name = table_name.strip()
            if table_name:
                table_names.append(table_name)

    return table_names


def load_table_names_from_file(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return parse_table_names(
        line for line in lines if line.strip() and not line.strip().startswith("#")
    )


def build_connection(config: PostgresConfig):
    import psycopg2

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
    )


def split_table_name(table_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in table_name.split(".")]

    if len(parts) == 1 and parts[0]:
        return DEFAULT_SCHEMA, parts[0]

    if len(parts) == 2 and all(parts):
        return parts[0], parts[1]

    raise ValueError(
        f"Nome de tabela invalido: {table_name}. Use tabela ou schema.tabela."
    )


def ensure_table_exists(connection, schema_name: str, table_name: str) -> None:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
              AND table_type = 'BASE TABLE'
        )
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        exists = cursor.fetchone()[0]

    if not exists:
        raise TableNotFoundError(f"Tabela nao encontrada: {schema_name}.{table_name}")


def extract_table(connection, table_name: str) -> ExtractedTable:
    from psycopg2 import sql

    schema_name, raw_table_name = split_table_name(table_name)
    ensure_table_exists(connection, schema_name, raw_table_name)

    query = sql.SQL("SELECT * FROM {}").format(
        sql.Identifier(schema_name, raw_table_name)
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [column.name for column in cursor.description]
        rows = cursor.fetchall()

    return ExtractedTable(
        schema_name=schema_name,
        table_name=raw_table_name,
        columns=columns,
        rows=rows,
    )


def extract_tables(
    config: PostgresConfig,
    table_names: Sequence[str],
) -> list[ExtractedTable]:
    with build_connection(config) as connection:
        return [extract_table(connection, table_name) for table_name in table_names]


def build_csv_path(output_dir: Path, table: ExtractedTable) -> Path:
    return output_dir / table.schema_name / f"{table.table_name}.csv"


def write_table_to_csv(output_dir: Path, table: ExtractedTable) -> Path:
    csv_path = build_csv_path(output_dir, table)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(table.columns)
        writer.writerows(table.rows)

    return csv_path


def write_tables_to_csv(
    output_dir: Path,
    tables: Sequence[ExtractedTable],
) -> list[tuple[ExtractedTable, Path]]:
    return [(table, write_table_to_csv(output_dir, table)) for table in tables]


def upload_written_tables(
    written_tables: Sequence[tuple[ExtractedTable, Path]],
) -> list[tuple[ExtractedTable, Path, str]]:
    minio_config = MinioConfig.from_env()
    minio_client = build_minio_client(minio_config)
    validate_bucket(minio_client, minio_config.bucket)

    uploaded_tables: list[tuple[ExtractedTable, Path, str]] = []
    for table, csv_path in written_tables:
        uploaded_object = upload_csv_to_landing(
            client=minio_client,
            bucket=minio_config.bucket,
            source_path=csv_path,
            schema_name=table.schema_name,
            table_name=table.table_name,
        )
        uploaded_tables.append((table, csv_path, uploaded_object.object_name))

    return uploaded_tables


def check_connection(config: PostgresConfig) -> None:
    with build_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Base da ingestao de tabelas do PostgreSQL para a landing.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=[],
        help="Lista de tabelas para ingestao. Aceita valores separados por espaco ou virgula.",
    )
    parser.add_argument(
        "--tables-file",
        type=Path,
        help="Arquivo texto com uma tabela por linha. Linhas iniciadas por # sao ignoradas.",
    )
    parser.add_argument(
        "--check-connection",
        action="store_true",
        help="Valida a conexao com o PostgreSQL antes de continuar.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/landing"),
        help="Diretorio local onde os CSVs brutos serao gravados.",
    )
    parser.add_argument(
        "--upload-minio",
        action="store_true",
        help="Envia os CSVs gerados para o bucket landing no MinIO.",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    config = PostgresConfig.from_env()

    table_names = parse_table_names(args.tables)
    if args.tables_file:
        table_names.extend(load_table_names_from_file(args.tables_file))

    if not table_names:
        raise SystemExit("Informe pelo menos uma tabela usando --tables ou --tables-file.")

    if args.check_connection:
        check_connection(config)

    try:
        extracted_tables = extract_tables(config, table_names)
    except (TableNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    written_tables = write_tables_to_csv(args.output_dir, extracted_tables)

    print(f"Banco de origem: {config.host}:{config.port}/{config.database}")
    print(f"Diretorio de saida: {args.output_dir}")
    print("CSVs gerados:")
    for table, csv_path in written_tables:
        print(
            f"- {csv_path}: {len(table.rows)} registros | "
            f"colunas: {', '.join(table.columns)}"
        )

    if args.upload_minio:
        try:
            uploaded_tables = upload_written_tables(written_tables)
        except (
            LandingPathError,
            MinioConfigError,
            MinioConnectionError,
        ) as exc:
            raise SystemExit(str(exc)) from exc

        print("CSVs enviados para o MinIO:")
        for table, csv_path, object_name in uploaded_tables:
            print(f"- {csv_path} -> {object_name} ({len(table.rows)} registros)")


if __name__ == "__main__":
    main()
