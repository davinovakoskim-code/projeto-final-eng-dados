import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


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
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=_required_env("POSTGRES_USER"),
            password=_required_env("POSTGRES_PASSWORD"),
            database=_required_env("POSTGRES_DB"),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


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
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = PostgresConfig.from_env()

    table_names = parse_table_names(args.tables)
    if args.tables_file:
        table_names.extend(load_table_names_from_file(args.tables_file))

    if not table_names:
        raise SystemExit("Informe pelo menos uma tabela usando --tables ou --tables-file.")

    if args.check_connection:
        check_connection(config)

    print(f"Banco de origem: {config.host}:{config.port}/{config.database}")
    print("Tabelas configuradas para ingestao:")
    for table_name in table_names:
        print(f"- {table_name}")


if __name__ == "__main__":
    main()
