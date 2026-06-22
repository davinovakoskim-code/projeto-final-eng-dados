import argparse
import os
from dataclasses import dataclass
from urllib.parse import urlparse


DEFAULT_MINIO_ENDPOINT = "localhost:9000"
DEFAULT_MINIO_BUCKET = "landing"


class MinioConfigError(RuntimeError):
    pass


class MinioConnectionError(RuntimeError):
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
