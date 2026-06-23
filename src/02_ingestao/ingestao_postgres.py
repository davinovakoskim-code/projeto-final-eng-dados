import argparse
import csv
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_SCHEMA = "public"
DEFAULT_OUTPUT_DIR = Path("data/landing")
DEFAULT_BUCKET = "landing"


@dataclass(frozen=True)
class ExtractedTable:
    name: str
    columns: list[str]
    rows: list[tuple]


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


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool

    @classmethod
    def from_env(cls) -> "MinioConfig":
        endpoint = _required_env("MINIO_ENDPOINT")
        if "://" in endpoint:
            endpoint = endpoint.split("://", 1)[1]

        return cls(
            endpoint=endpoint,
            access_key=_required_env("MINIO_ROOT_USER"),
            secret_key=_required_env("MINIO_ROOT_PASSWORD"),
            bucket=os.getenv("MINIO_BUCKET", DEFAULT_BUCKET),
            secure=_env_bool("MINIO_SECURE", False),
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
        name=f"{schema_name}.{raw_table_name}",
        columns=columns,
        rows=rows,
    )


def extract_tables(
    config: PostgresConfig,
    table_names: Sequence[str],
) -> list[ExtractedTable]:
    with build_connection(config) as connection:
        return [extract_table(connection, table_name) for table_name in table_names]


def _safe_file_name(table_name: str) -> str:
    """Converte schema.tabela em um nome de arquivo seguro (schema__tabela)."""
    return table_name.replace(".", "__")


def write_table_csv(table: ExtractedTable, destination_dir: Path) -> Path:
    """Escreve uma tabela como CSV bruto e retorna o caminho gerado.

    A escrita e atomica: grava em um arquivo temporario e renomeia ao final,
    garantindo idempotencia (reexecutar sobrescreve sem deixar arquivo parcial).
    """
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / f"{_safe_file_name(table.name)}.csv"
    tmp_destination = destination.with_suffix(".csv.tmp")

    with tmp_destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(table.columns)
        writer.writerows(table.rows)

    tmp_destination.replace(destination)
    return destination


def write_tables_csv(
    tables: Sequence[ExtractedTable],
    output_dir: Path,
    extraction_date: date,
) -> list[Path]:
    """Escreve todas as tabelas em output_dir/extraction_date=YYYY-MM-DD/."""
    partition_dir = output_dir / f"extraction_date={extraction_date.isoformat()}"
    return [write_table_csv(table, partition_dir) for table in tables]


def build_minio_client(config: MinioConfig):
    from minio import Minio

    return Minio(
        config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
    )


def ensure_bucket_exists(client, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_files_to_minio(
    config: MinioConfig,
    files: Sequence[Path],
    output_dir: Path,
) -> list[str]:
    """Sobe os CSVs para o bucket, preservando a estrutura de pastas relativa.

    Retorna a lista de object names criados (idempotente: put_object sobrescreve).
    """
    client = build_minio_client(config)
    ensure_bucket_exists(client, config.bucket)

    object_names: list[str] = []
    for file_path in files:
        object_name = file_path.relative_to(output_dir).as_posix()
        client.fput_object(
            config.bucket,
            object_name,
            str(file_path),
            content_type="text/csv",
        )
        object_names.append(object_name)

    return object_names


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
        default=DEFAULT_OUTPUT_DIR,
        help="Diretorio local onde os CSVs da landing serao gravados.",
    )
    parser.add_argument(
        "--extraction-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Data da extracao (YYYY-MM-DD) usada para particionar a landing. Padrao: hoje.",
    )
    parser.add_argument(
        "--upload-minio",
        action="store_true",
        help="Apos gerar os CSVs, faz upload para o bucket landing no MinIO.",
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

    print(f"Banco de origem: {config.host}:{config.port}/{config.database}")
    print("Tabelas extraidas:")
    for table in extracted_tables:
        print(
            f"- {table.name}: {len(table.rows)} registros | "
            f"colunas: {', '.join(table.columns)}"
        )

    written_files = write_tables_csv(
        extracted_tables, args.output_dir, args.extraction_date
    )
    print(
        f"\nCSVs gravados em {args.output_dir}/"
        f"extraction_date={args.extraction_date.isoformat()}/ "
        f"({len(written_files)} arquivos)."
    )

    if args.upload_minio:
        minio_config = MinioConfig.from_env()
        object_names = upload_files_to_minio(
            minio_config, written_files, args.output_dir
        )
        print(
            f"Upload concluido para o bucket '{minio_config.bucket}' "
            f"({len(object_names)} objetos):"
        )
        for object_name in object_names:
            print(f"- {object_name}")


if __name__ == "__main__":
    main()
