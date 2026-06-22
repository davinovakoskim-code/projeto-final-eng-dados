import argparse
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_MINIO_ENDPOINT = "localhost:9000"
DEFAULT_MINIO_BUCKET = "landing"
DEFAULT_LANDING_FILE_EXTENSION = "csv"


class MinioConfigError(RuntimeError):
    pass


class MinioConnectionError(RuntimeError):
    pass


class LandingPathError(ValueError):
    pass


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool

    @classmethod
    def from_env(cls) -> "MinioConfig":
        endpoint, secure_from_endpoint = normalize_endpoint(
            os.getenv("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT)
        )

        secure = (
            secure_from_endpoint
            if secure_from_endpoint is not None
            else env_bool("MINIO_SECURE", False)
        )

        return cls(
            endpoint=endpoint,
            access_key=required_env("MINIO_ACCESS_KEY", "MINIO_ROOT_USER"),
            secret_key=required_env("MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD"),
            bucket=os.getenv("MINIO_BUCKET", DEFAULT_MINIO_BUCKET),
            secure=secure,
        )


@dataclass(frozen=True)
class UploadedObject:
    source_path: Path
    bucket: str
    object_name: str


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


def required_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value

    raise MinioConfigError(
        "Variavel de ambiente obrigatoria nao definida: " + " ou ".join(names)
    )


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    value = value.strip().lower()
    if value in {"1", "true", "t", "yes", "y", "sim", "s"}:
        return True
    if value in {"0", "false", "f", "no", "n", "nao"}:
        return False

    raise MinioConfigError(f"Variavel de ambiente {name} deve ser booleana.")


def normalize_endpoint(endpoint: str) -> tuple[str, bool | None]:
    endpoint = endpoint.strip()
    if not endpoint:
        raise MinioConfigError("MINIO_ENDPOINT nao pode ser vazio.")

    if "://" not in endpoint:
        return endpoint, None

    parsed_endpoint = urlparse(endpoint)
    if parsed_endpoint.scheme not in {"http", "https"}:
        raise MinioConfigError(
            "MINIO_ENDPOINT deve usar http, https ou somente host:porta."
        )
    if parsed_endpoint.path not in {"", "/"}:
        raise MinioConfigError("MINIO_ENDPOINT nao deve conter caminho.")
    if not parsed_endpoint.netloc:
        raise MinioConfigError("MINIO_ENDPOINT deve conter host e porta.")

    return parsed_endpoint.netloc, parsed_endpoint.scheme == "https"


def validate_path_part(value: str, field_name: str) -> str:
    value = value.strip()
    if not value:
        raise LandingPathError(f"{field_name} nao pode ser vazio.")
    if "/" in value:
        raise LandingPathError(f"{field_name} nao pode conter barra (/).")

    return value


def build_landing_object_name(
    schema_name: str,
    table_name: str,
    extraction_date: date | None = None,
    file_extension: str = DEFAULT_LANDING_FILE_EXTENSION,
) -> str:
    schema_name = validate_path_part(schema_name, "schema_name")
    table_name = validate_path_part(table_name, "table_name")
    file_extension = validate_path_part(file_extension.lstrip("."), "file_extension")
    extraction_date = extraction_date or date.today()

    return (
        f"{schema_name}/{table_name}/"
        f"data_extracao={extraction_date.isoformat()}/"
        f"{table_name}.{file_extension}"
    )


def build_minio_client(config: MinioConfig):
    try:
        from minio import Minio
    except ImportError as exc:
        raise MinioConfigError(
            "Dependencia minio nao instalada. Rode: uv sync"
        ) from exc

    return Minio(
        endpoint=config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
    )


def validate_bucket(client, bucket: str) -> None:
    try:
        bucket_exists = client.bucket_exists(bucket)
    except Exception as exc:
        raise MinioConnectionError(
            f"Falha ao validar bucket {bucket} no MinIO: {exc}"
        ) from exc

    if not bucket_exists:
        raise MinioConnectionError(f"Bucket nao encontrado no MinIO: {bucket}")


def check_minio_connection(config: MinioConfig) -> None:
    client = build_minio_client(config)
    validate_bucket(client, config.bucket)


def upload_file(
    client,
    bucket: str,
    source_path: Path,
    object_name: str,
    content_type: str = "text/csv",
) -> UploadedObject:
    source_path = source_path.resolve()
    if not source_path.is_file():
        raise MinioConnectionError(f"Arquivo local nao encontrado: {source_path}")

    try:
        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=str(source_path),
            content_type=content_type,
        )
    except Exception as exc:
        raise MinioConnectionError(
            f"Falha ao enviar {source_path} para {bucket}/{object_name}: {exc}"
        ) from exc

    return UploadedObject(
        source_path=source_path,
        bucket=bucket,
        object_name=object_name,
    )


def upload_csv_to_landing(
    client,
    bucket: str,
    source_path: Path,
    schema_name: str,
    table_name: str,
    extraction_date: date | None = None,
) -> UploadedObject:
    object_name = build_landing_object_name(
        schema_name=schema_name,
        table_name=table_name,
        extraction_date=extraction_date,
        file_extension="csv",
    )

    return upload_file(
        client=client,
        bucket=bucket,
        source_path=source_path,
        object_name=object_name,
        content_type="text/csv",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Configura e valida o client do MinIO para a landing.",
    )
    parser.add_argument(
        "--check-connection",
        action="store_true",
        help="Valida a conexao com o MinIO e a existencia do bucket configurado.",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    try:
        config = MinioConfig.from_env()
    except MinioConfigError as exc:
        raise SystemExit(str(exc)) from exc

    if args.check_connection:
        try:
            check_minio_connection(config)
        except (MinioConfigError, MinioConnectionError) as exc:
            raise SystemExit(str(exc)) from exc

    print(f"MinIO configurado: {config.endpoint}")
    print(f"Bucket de destino: {config.bucket}")
    print(f"Conexao segura: {config.secure}")


if __name__ == "__main__":
    main()
