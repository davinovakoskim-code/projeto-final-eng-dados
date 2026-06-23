"""
Transformacao Bronze -> Silver (limpeza / refino / Data Quality).

Le as tabelas Delta da camada Bronze (geradas por ``landing_to_bronze.py``, onde
tudo e string fiel a origem) e grava versoes refinadas na camada Silver:

  - Tipagem correta (cast por coluna, espelhando o DDL de ``src/01_origem/schema.sql``).
  - Padronizacao: trim em strings, string vazia -> NULL, datas/timestamps nativos,
    dominios normalizados (minusculo/trim).
  - Tratamento de nulos obrigatorios (colunas NOT NULL do DDL).
  - Deduplicacao pela chave primaria.
  - Coerencia temporal (ex.: data_fim >= data_inicio).

Registros que violam regras de qualidade NAO sao descartados silenciosamente: vao
para uma area de QUARENTENA em Delta, preservando o dado e o motivo da rejeicao.

  Silver "limpa":   s3a://silver/<tabela>/
  Quarentena:       s3a://silver/_quarentena/<tabela>/  (+ coluna _motivo_rejeicao)

Integridade referencial entre tabelas (FK cruzando dimensao x fato) NAO e validada
aqui: numa arquitetura medalhao isso pertence aos joins da Gold (modelo Kimball).
A Silver foca em qualidade *por tabela*.

Uso:
    python src/03_transformacao/bronze_to_silver.py
    python src/03_transformacao/bronze_to_silver.py --tables streamers assinaturas
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

# Garante que src/ esteja no path para importar utils.spark_config, tanto local
# quanto dentro do container Jupyter (/home/jovyan/work/src).
_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.spark_config import build_spark_session  # noqa: E402


DEFAULT_BRONZE_BUCKET = "bronze"
DEFAULT_SILVER_BUCKET = "silver"
QUARANTINE_PREFIX = "_quarentena"


# ----------------------------------------------------------------------
# Spec declarativa por tabela (derivada 1:1 de src/01_origem/schema.sql).
#
# - columns: mapa coluna -> tipo Spark alvo. A ordem reflete o DDL.
#   Tipos: "int", "decimal(8,2)", "date", "timestamp", "boolean", "string".
# - pk: chave primaria (usada para deduplicacao e como coluna NOT NULL).
# - not_null: colunas NOT NULL do DDL (alem da PK). Nulo aqui -> quarentena.
# - domains: coluna -> conjunto de valores aceitos (CHECK do DDL).
# - temporal: lista de (col_fim, col_inicio); exige col_fim >= col_inicio quando
#   col_fim nao for nula.
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class TableSpec:
    pk: str
    columns: dict[str, str]
    not_null: tuple[str, ...] = ()
    domains: dict[str, frozenset[str]] = field(default_factory=dict)
    temporal: tuple[tuple[str, str], ...] = ()


TABLE_SPECS: dict[str, TableSpec] = {
    "plataformas": TableSpec(
        pk="id_plataforma",
        columns={"id_plataforma": "int", "nome": "string"},
        not_null=("nome",),
    ),
    "jogos": TableSpec(
        pk="id_jogo",
        columns={
            "id_jogo": "int",
            "nome": "string",
            "desenvolvedor": "string",
            "ano_lancamento": "int",
            "ativo": "boolean",
        },
        not_null=("nome",),
    ),
    "streamers": TableSpec(
        pk="id_streamer",
        columns={
            "id_streamer": "int",
            "nome": "string",
            "pais": "string",
            "data_cadastro": "date",
            "id_plataforma": "int",
        },
        not_null=("nome", "data_cadastro"),
    ),
    "viewers": TableSpec(
        pk="id_viewer",
        columns={
            "id_viewer": "int",
            "nome": "string",
            "pais": "string",
            "data_cadastro": "date",
        },
        not_null=("nome", "data_cadastro"),
    ),
    "emotes": TableSpec(
        pk="id_emote",
        columns={
            "id_emote": "int",
            "nome": "string",
            "id_streamer": "int",
            "disponivel_para": "string",
        },
        not_null=("nome",),
        domains={"disponivel_para": frozenset({"gratis", "assinante"})},
    ),
    "transmissoes": TableSpec(
        pk="id_transmissao",
        columns={
            "id_transmissao": "int",
            "id_streamer": "int",
            "id_jogo": "int",
            "data_inicio": "timestamp",
            "data_fim": "timestamp",
            "pico_viewers": "int",
        },
        not_null=("data_inicio", "data_fim"),
        temporal=(("data_fim", "data_inicio"),),
    ),
    "visualizacoes": TableSpec(
        pk="id_visualizacao",
        columns={
            "id_visualizacao": "int",
            "id_viewer": "int",
            "id_transmissao": "int",
            "minutos_assistidos": "int",
            "data_hora": "timestamp",
        },
        not_null=("data_hora",),
    ),
    "follows": TableSpec(
        pk="id_follow",
        columns={
            "id_follow": "int",
            "id_viewer": "int",
            "id_streamer": "int",
            "data_follow": "date",
            "data_unfollow": "date",
        },
        not_null=("data_follow",),
        temporal=(("data_unfollow", "data_follow"),),
    ),
    "assinaturas": TableSpec(
        pk="id_assinatura",
        columns={
            "id_assinatura": "int",
            "id_viewer": "int",
            "id_streamer": "int",
            "tipo": "string",
            "data_inicio": "date",
            "data_fim": "date",
            "valor_mensal": "decimal(8,2)",
        },
        not_null=("data_inicio",),
        domains={"tipo": frozenset({"gratis", "tier1", "tier2", "tier3"})},
        temporal=(("data_fim", "data_inicio"),),
    ),
    "doacoes": TableSpec(
        pk="id_doacao",
        columns={
            "id_doacao": "int",
            "id_viewer": "int",
            "id_streamer": "int",
            "id_transmissao": "int",
            "valor": "decimal(8,2)",
            "data_hora": "timestamp",
        },
        not_null=("valor", "data_hora"),
    ),
    "clips": TableSpec(
        pk="id_clip",
        columns={
            "id_clip": "int",
            "id_transmissao": "int",
            "id_viewer": "int",
            "visualizacoes": "int",
            "data_criacao": "timestamp",
        },
        not_null=("data_criacao",),
    ),
    "raids": TableSpec(
        pk="id_raid",
        columns={
            "id_raid": "int",
            "id_streamer_origem": "int",
            "id_streamer_destino": "int",
            "id_transmissao": "int",
            "viewers_enviados": "int",
            "data_hora": "timestamp",
        },
        not_null=("data_hora",),
    ),
    "moderadores": TableSpec(
        pk="id_moderador",
        columns={
            "id_moderador": "int",
            "id_viewer": "int",
            "id_streamer": "int",
            "data_inicio": "date",
            "data_fim": "date",
        },
        not_null=("data_inicio",),
        temporal=(("data_fim", "data_inicio"),),
    ),
}

# Colunas de auditoria geradas pela Bronze; descartadas antes do refino (a Silver
# tem a sua propria proveniencia).
BRONZE_AUDIT_COLUMNS = ("_source_file", "_ingestion_timestamp")


@dataclass(frozen=True)
class SilverConfig:
    """Configuracao dos buckets origem (Bronze) e destino (Silver)."""

    bronze_bucket: str
    silver_bucket: str

    @classmethod
    def from_env(cls) -> "SilverConfig":
        # Apenas valida que as credenciais do MinIO existem; o acesso real ao
        # storage e via s3a (configurado em build_spark_session).
        _required_env("MINIO_ROOT_USER")
        _required_env("MINIO_ROOT_PASSWORD")
        return cls(
            bronze_bucket=os.getenv("MINIO_BRONZE_BUCKET", DEFAULT_BRONZE_BUCKET),
            silver_bucket=os.getenv("MINIO_SILVER_BUCKET", DEFAULT_SILVER_BUCKET),
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


def parse_table_names(values) -> list[str]:
    table_names: list[str] = []
    for value in values:
        for table_name in value.split(","):
            table_name = table_name.strip()
            if table_name:
                table_names.append(table_name)
    return table_names


def cast_columns(df, spec: TableSpec, keep: Sequence[str] = ()):
    """Aplica padronizacao de string + cast de tipos conforme a spec.

    - Em colunas string: trim e converte string vazia (ausencia no CSV) em NULL.
    - Em colunas de dominio (string): normaliza para minusculo.
    - Casts invalidos viram NULL (capturado depois pela validacao de not_null).
    - ``keep``: colunas extras do DataFrame de origem a preservar como estao
      (ex.: ``_extraction_date`` propagada da Bronze).
    """
    from pyspark.sql import functions as F

    selected = []
    for column, target_type in spec.columns.items():
        col = F.col(column)

        if target_type == "string":
            cleaned = F.trim(col)
            cleaned = F.when(cleaned == "", None).otherwise(cleaned)
            if column in spec.domains:
                cleaned = F.lower(cleaned)
            selected.append(cleaned.alias(column))
        elif target_type == "boolean":
            # Parse robusto: trata 't'/'true'/'1' como true, 'f'/'false'/'0' como false.
            normalized = F.lower(F.trim(col))
            parsed = (
                F.when(normalized.isin("true", "t", "1", "yes", "y"), F.lit(True))
                .when(normalized.isin("false", "f", "0", "no", "n"), F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
            selected.append(parsed.alias(column))
        else:
            # int / decimal(p,s) / date / timestamp: cast direto. Vazio -> NULL antes.
            cleaned = F.when(F.trim(col) == "", None).otherwise(F.trim(col))
            selected.append(cleaned.cast(target_type).alias(column))

    for column in keep:
        if column in df.columns:
            selected.append(F.col(column))

    return df.select(*selected)


def evaluate_quality(df, spec: TableSpec):
    """Adiciona a coluna ``_motivo_rejeicao`` com as regras de qualidade violadas.

    Cada linha recebe a lista (compactada em string) dos motivos de rejeicao. Linhas
    sem motivo (string vazia/NULL) sao consideradas validas.
    """
    from pyspark.sql import functions as F

    motivos = []

    # PK nula (nao deveria existir; protege a deduplicacao).
    motivos.append(
        F.when(F.col(spec.pk).isNull(), F.lit(f"pk_nula:{spec.pk}"))
    )

    # Colunas NOT NULL do DDL.
    for column in spec.not_null:
        motivos.append(
            F.when(F.col(column).isNull(), F.lit(f"nulo_obrigatorio:{column}"))
        )

    # Dominios (CHECK do DDL): so reprova quando o valor existe e esta fora do dominio.
    for column, allowed in spec.domains.items():
        in_domain = F.col(column).isin(*sorted(allowed))
        motivos.append(
            F.when(
                F.col(column).isNotNull() & ~in_domain,
                F.lit(f"dominio_invalido:{column}"),
            )
        )

    # Coerencia temporal: col_fim >= col_inicio quando col_fim nao for nula.
    for col_fim, col_inicio in spec.temporal:
        motivos.append(
            F.when(
                F.col(col_fim).isNotNull()
                & F.col(col_inicio).isNotNull()
                & (F.col(col_fim) < F.col(col_inicio)),
                F.lit(f"temporal_invalido:{col_fim}<{col_inicio}"),
            )
        )

    # array_compact remove os NULLs (regras que passaram); concat_ws junta o resto.
    motivo_array = F.array_compact(F.array(*motivos))
    motivo_str = F.concat_ws("; ", motivo_array)
    motivo_str = F.when(motivo_str == "", None).otherwise(motivo_str)

    return df.withColumn("_motivo_rejeicao", motivo_str)


def split_valid_quarantine(df):
    """Separa o DataFrame em (validos, quarentena) pela coluna ``_motivo_rejeicao``."""
    from pyspark.sql import functions as F

    quarantine = df.filter(F.col("_motivo_rejeicao").isNotNull())
    valid = df.filter(F.col("_motivo_rejeicao").isNull()).drop("_motivo_rejeicao")
    return valid, quarantine


def deduplicate(df, spec: TableSpec):
    """Remove duplicados pela PK, mantendo uma linha por chave.

    Retorna (df_unico, n_duplicados_removidos).
    """
    total = df.count()
    deduped = df.dropDuplicates([spec.pk])
    unique = deduped.count()
    return deduped, total - unique


def add_silver_audit(df):
    from pyspark.sql import functions as F

    return df.withColumn("_silver_processed_at", F.current_timestamp())


def write_delta(df, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


@dataclass(frozen=True)
class TableResult:
    table: str
    read: int
    valid: int
    quarantined: int
    duplicates: int


def transform_table_to_silver(
    spark,
    config: SilverConfig,
    table: str,
) -> TableResult:
    """Le a Bronze de uma tabela, aplica Data Quality e grava na Silver.

    Validos vao para s3a://silver/<tabela>/; rejeitados para a quarentena.
    """
    spec = TABLE_SPECS[table]
    bronze_path = f"s3a://{config.bronze_bucket}/{table}"
    silver_path = f"s3a://{config.silver_bucket}/{table}"
    quarantine_path = f"s3a://{config.silver_bucket}/{QUARANTINE_PREFIX}/{table}"

    bronze = spark.read.format("delta").load(bronze_path)
    read_count = bronze.count()

    # Descarta a auditoria propria da Bronze; propaga _extraction_date para a Silver.
    drop_cols = [c for c in BRONZE_AUDIT_COLUMNS if c in bronze.columns]
    if drop_cols:
        bronze = bronze.drop(*drop_cols)

    # Tipagem + padronizacao (mantendo _extraction_date como veio da Bronze).
    typed = cast_columns(bronze, spec, keep=("_extraction_date",))

    # Avalia qualidade e separa validos x quarentena.
    evaluated = evaluate_quality(typed, spec)
    valid, quarantine = split_valid_quarantine(evaluated)

    # Deduplicacao pela PK (apenas nos validos).
    valid, duplicates = deduplicate(valid, spec)

    valid = add_silver_audit(valid)
    valid_count = valid.count()
    write_delta(valid, silver_path)

    quarantine = add_silver_audit(quarantine)
    quarantine_count = quarantine.count()
    write_delta(quarantine, quarantine_path)

    return TableResult(
        table=table,
        read=read_count,
        valid=valid_count,
        quarantined=quarantine_count,
        duplicates=duplicates,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Le as tabelas Delta da Bronze e grava versoes refinadas (tipagem, "
            "nulos, duplicados, dominios, coerencia temporal) na Silver. Registros "
            "invalidos vao para a quarentena."
        ),
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=[],
        help=(
            "Tabelas a processar (separadas por espaco ou virgula). Por padrao, "
            "processa todas as tabelas conhecidas."
        ),
    )
    parser.add_argument(
        "--bronze-bucket",
        help="Sobrescreve o bucket de origem (padrao: MINIO_BRONZE_BUCKET ou 'bronze').",
    )
    parser.add_argument(
        "--silver-bucket",
        help="Sobrescreve o bucket de destino (padrao: MINIO_SILVER_BUCKET ou 'silver').",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    config = SilverConfig.from_env()

    if args.bronze_bucket:
        config = SilverConfig(**{**config.__dict__, "bronze_bucket": args.bronze_bucket})
    if args.silver_bucket:
        config = SilverConfig(**{**config.__dict__, "silver_bucket": args.silver_bucket})

    selected = parse_table_names(args.tables) or list(TABLE_SPECS.keys())

    unknown = [t for t in selected if t not in TABLE_SPECS]
    if unknown:
        raise SystemExit(
            f"Tabela(s) desconhecida(s): {', '.join(unknown)}. "
            f"Conhecidas: {', '.join(TABLE_SPECS.keys())}."
        )

    print(f"Bronze: s3a://{config.bronze_bucket}/<tabela>/")
    print(f"Silver: s3a://{config.silver_bucket}/<tabela>/")
    print(
        f"Quarentena: s3a://{config.silver_bucket}/{QUARANTINE_PREFIX}/<tabela>/"
    )
    print(f"Tabelas a processar: {len(selected)}\n")

    spark = build_spark_session(app_name="bronze-to-silver")
    try:
        results = []
        for table in selected:
            result = transform_table_to_silver(spark, config, table)
            results.append(result)
            print(
                f"- {result.table}: lidos={result.read} | validos={result.valid} | "
                f"quarentena={result.quarantined} | duplicados removidos={result.duplicates}"
            )
    finally:
        spark.stop()

    total_quarentena = sum(r.quarantined for r in results)
    print(
        f"\nTransformacao Bronze -> Silver concluida. "
        f"Registros em quarentena: {total_quarentena}."
    )


if __name__ == "__main__":
    main()
