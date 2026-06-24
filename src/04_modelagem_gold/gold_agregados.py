"""
Agregados Gold -- data marts (joins + agregacoes do modelo estrela).

Segundo estagio da camada Gold. Enquanto ``silver_to_gold.py`` materializa o
star schema (dimensoes + fatos no grao da transacao), este script consome esse
star schema e materializa TABELAS AGREGADAS (data marts), ja com os joins
fato x dimensao e as agregacoes resolvidas. Sao as tabelas que o dashboard le
direto, sem precisar refazer joins/group by a cada consulta.

  Gold star schema (origem):  s3a://gold/<dim_ou_fato>/
  Gold agregados   (destino): s3a://gold/agg_<mart>/

Marts implementados:
    - agg_streamer_visao_geral : 1 linha por streamer (KPIs consolidados).
    - agg_receita_mensal       : 1 linha por mes (serie temporal de receita).
    - agg_jogo_popularidade    : 1 linha por jogo (audiencia e transmissoes).
    - agg_plataforma_resumo    : 1 linha por plataforma (visao macro).

Membro desconhecido: as dimensoes da Gold tem a linha sk = -1; os marts a
ignoram (joins partem da dimensao filtrando sk != -1), de modo que linhas de
fato orfas nao poluem os agregados.

Uso:
    python src/04_modelagem_gold/gold_agregados.py
    python src/04_modelagem_gold/gold_agregados.py --marts agg_receita_mensal
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

# Garante que src/ esteja no path para importar utils.spark_config, tanto local
# quanto dentro do container Jupyter (/home/jovyan/work/src).
_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.spark_config import build_spark_session  # noqa: E402


DEFAULT_GOLD_BUCKET = "gold"
UNKNOWN_SK = -1

ALL_MARTS = (
    "agg_streamer_visao_geral",
    "agg_receita_mensal",
    "agg_jogo_popularidade",
    "agg_plataforma_resumo",
)


@dataclass(frozen=True)
class AggConfig:
    """Configuracao do bucket Gold (origem do star schema e destino dos marts)."""

    gold_bucket: str

    @classmethod
    def from_env(cls) -> "AggConfig":
        _required_env("MINIO_ROOT_USER")
        _required_env("MINIO_ROOT_PASSWORD")
        return cls(gold_bucket=os.getenv("MINIO_GOLD_BUCKET", DEFAULT_GOLD_BUCKET))


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


# ----------------------------------------------------------------------
# Leitura / escrita
# ----------------------------------------------------------------------


def read_gold(spark, config: AggConfig, obj: str):
    """Le uma tabela Delta do star schema da Gold (sem a coluna de auditoria)."""
    path = f"s3a://{config.gold_bucket}/{obj}"
    df = spark.read.format("delta").load(path)
    return df.drop("_gold_processed_at") if "_gold_processed_at" in df.columns else df


def add_gold_audit(df):
    from pyspark.sql import functions as F

    return df.withColumn("_gold_processed_at", F.current_timestamp())


def write_delta(df, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def _coalesce_zero(df, columns):
    """Substitui NULL por 0 nas metricas (gerados por left joins sem match)."""
    from pyspark.sql import functions as F

    for column in columns:
        df = df.withColumn(column, F.coalesce(F.col(column), F.lit(0)))
    return df


# ----------------------------------------------------------------------
# Marts (joins + agregacoes conforme o modelo dimensional)
# ----------------------------------------------------------------------


def build_agg_streamer_visao_geral(spark, gold):
    """1 linha por streamer com os KPIs de todas as areas (transmissao,
    audiencia, doacao e assinatura) consolidados via dimensao conformada.
    """
    from pyspark.sql import functions as F

    dim = gold["dim_streamer"].filter(F.col("sk_streamer") != UNKNOWN_SK)

    transm = (
        gold["fato_transmissoes"]
        .groupBy("sk_streamer")
        .agg(
            F.count("*").alias("qtd_transmissoes"),
            F.round(F.avg("pico_viewers"), 1).alias("pico_viewers_medio"),
            F.round(F.sum("duracao_minutos") / 60.0, 1).alias("horas_transmitidas"),
        )
    )
    audiencia = (
        gold["fato_visualizacoes"]
        .groupBy("sk_streamer")
        .agg(
            F.sum("minutos_assistidos").alias("minutos_assistidos"),
            F.countDistinct("sk_viewer").alias("viewers_unicos"),
        )
    )
    doacoes = (
        gold["fato_doacoes"]
        .groupBy("sk_streamer")
        .agg(
            F.round(F.sum("valor"), 2).alias("total_doacoes"),
            F.count("*").alias("qtd_doacoes"),
        )
    )
    assinaturas = (
        gold["fato_assinaturas"]
        .filter(F.col("ativa"))
        .groupBy("sk_streamer")
        .agg(
            F.count("*").alias("assinaturas_ativas"),
            F.round(F.sum("valor_mensal"), 2).alias("mrr"),
        )
    )

    mart = (
        dim.select("sk_streamer", "nome_streamer", "pais", "nome_plataforma")
        .join(transm, "sk_streamer", "left")
        .join(audiencia, "sk_streamer", "left")
        .join(doacoes, "sk_streamer", "left")
        .join(assinaturas, "sk_streamer", "left")
    )
    metricas = [
        "qtd_transmissoes", "pico_viewers_medio", "horas_transmitidas",
        "minutos_assistidos", "viewers_unicos",
        "total_doacoes", "qtd_doacoes", "assinaturas_ativas", "mrr",
    ]
    mart = _coalesce_zero(mart, metricas)
    return mart.orderBy(F.col("total_doacoes").desc())


def build_agg_receita_mensal(spark, gold):
    """1 linha por mes (ano, mes) com a serie temporal de receita.

    Doacoes pela data da doacao; assinaturas pela data de inicio (novas
    assinaturas e receita mensal recorrente adicionada no mes).
    """
    from pyspark.sql import functions as F

    # So ano/mes nas agregacoes; nome_mes e derivado depois do full_outer para
    # nao ficar nulo nos meses presentes em apenas um dos lados.
    tempo = gold["dim_tempo"].select("sk_tempo", "ano", "mes")

    doacoes = (
        gold["fato_doacoes"]
        .join(tempo, "sk_tempo")
        .groupBy("ano", "mes")
        .agg(
            F.round(F.sum("valor"), 2).alias("receita_doacoes"),
            F.count("*").alias("qtd_doacoes"),
        )
    )
    assinaturas = (
        gold["fato_assinaturas"]
        .join(tempo, "sk_tempo")
        .groupBy("ano", "mes")
        .agg(
            F.count("*").alias("novas_assinaturas"),
            F.round(F.sum("valor_mensal"), 2).alias("receita_assinaturas"),
        )
    )

    mart = (
        doacoes.join(assinaturas, ["ano", "mes"], "full_outer")
    )
    mart = _coalesce_zero(
        mart,
        ["receita_doacoes", "qtd_doacoes", "novas_assinaturas", "receita_assinaturas"],
    )
    mart = mart.withColumn(
        "receita_total",
        F.round(F.col("receita_doacoes") + F.col("receita_assinaturas"), 2),
    ).withColumn(
        "nome_mes",
        F.date_format(F.make_date(F.col("ano"), F.col("mes"), F.lit(1)), "MMMM"),
    )
    return mart.select(
        "ano", "mes", "nome_mes", "receita_doacoes", "qtd_doacoes",
        "novas_assinaturas", "receita_assinaturas", "receita_total",
    ).orderBy("ano", "mes")


def build_agg_jogo_popularidade(spark, gold):
    """1 linha por jogo: volume de transmissoes e audiencia agregada."""
    from pyspark.sql import functions as F

    dim = gold["dim_jogo"].filter(F.col("sk_jogo") != UNKNOWN_SK)

    transm = (
        gold["fato_transmissoes"]
        .groupBy("sk_jogo")
        .agg(
            F.count("*").alias("qtd_transmissoes"),
            F.countDistinct("sk_streamer").alias("streamers_distintos"),
            F.round(F.avg("pico_viewers"), 1).alias("pico_viewers_medio"),
            F.round(F.avg("duracao_minutos"), 1).alias("duracao_media_min"),
        )
    )
    audiencia = (
        gold["fato_visualizacoes"]
        .groupBy("sk_jogo")
        .agg(F.sum("minutos_assistidos").alias("minutos_assistidos"))
    )

    mart = (
        dim.select("sk_jogo", "nome_jogo", "desenvolvedor", "ano_lancamento")
        .join(transm, "sk_jogo", "left")
        .join(audiencia, "sk_jogo", "left")
    )
    mart = _coalesce_zero(
        mart,
        ["qtd_transmissoes", "streamers_distintos", "pico_viewers_medio",
         "duracao_media_min", "minutos_assistidos"],
    )
    return mart.orderBy(F.col("qtd_transmissoes").desc())


def build_agg_plataforma_resumo(spark, gold):
    """1 linha por plataforma: visao macro (streamers, audiencia, doacoes).

    Doacoes e visualizacoes nao tem FK direta para plataforma; a plataforma e
    obtida atraves de dim_streamer (sk_streamer -> sk_plataforma).
    """
    from pyspark.sql import functions as F

    dim_streamer = gold["dim_streamer"].filter(F.col("sk_streamer") != UNKNOWN_SK)
    streamer_plat = dim_streamer.select("sk_streamer", "sk_plataforma")

    qtd_streamers = (
        dim_streamer.groupBy("sk_plataforma")
        .agg(F.count("*").alias("qtd_streamers"))
    )
    doacoes = (
        gold["fato_doacoes"]
        .join(streamer_plat, "sk_streamer")
        .groupBy("sk_plataforma")
        .agg(F.round(F.sum("valor"), 2).alias("total_doacoes"))
    )
    audiencia = (
        gold["fato_visualizacoes"]
        .join(streamer_plat, "sk_streamer")
        .groupBy("sk_plataforma")
        .agg(F.sum("minutos_assistidos").alias("minutos_assistidos"))
    )

    dim_plat = gold["dim_plataforma"].filter(F.col("sk_plataforma") != UNKNOWN_SK)
    mart = (
        dim_plat.select("sk_plataforma", "nome_plataforma")
        .join(qtd_streamers, "sk_plataforma", "left")
        .join(doacoes, "sk_plataforma", "left")
        .join(audiencia, "sk_plataforma", "left")
    )
    mart = _coalesce_zero(
        mart, ["qtd_streamers", "total_doacoes", "minutos_assistidos"]
    )
    return mart.orderBy(F.col("total_doacoes").desc())


MART_BUILDERS = {
    "agg_streamer_visao_geral": build_agg_streamer_visao_geral,
    "agg_receita_mensal": build_agg_receita_mensal,
    "agg_jogo_popularidade": build_agg_jogo_popularidade,
    "agg_plataforma_resumo": build_agg_plataforma_resumo,
}

# Tabelas do star schema (Gold) necessarias para construir cada mart.
GOLD_DEPENDENCIES = {
    "agg_streamer_visao_geral": (
        "dim_streamer", "fato_transmissoes", "fato_visualizacoes",
        "fato_doacoes", "fato_assinaturas",
    ),
    "agg_receita_mensal": ("dim_tempo", "fato_doacoes", "fato_assinaturas"),
    "agg_jogo_popularidade": ("dim_jogo", "fato_transmissoes", "fato_visualizacoes"),
    "agg_plataforma_resumo": (
        "dim_plataforma", "dim_streamer", "fato_doacoes", "fato_visualizacoes",
    ),
}


# ----------------------------------------------------------------------
# Orquestracao
# ----------------------------------------------------------------------


def parse_names(values) -> list[str]:
    names: list[str] = []
    for value in values:
        for name in value.split(","):
            name = name.strip()
            if name:
                names.append(name)
    return names


@dataclass(frozen=True)
class MartResult:
    name: str
    rows: int


def run(spark, config: AggConfig, marts) -> list[MartResult]:
    needed: set[str] = set()
    for mart in marts:
        needed.update(GOLD_DEPENDENCIES[mart])

    print(f"Lendo Gold (star schema): {', '.join(sorted(needed))}\n")
    gold = {obj: read_gold(spark, config, obj) for obj in sorted(needed)}

    results: list[MartResult] = []
    for name in marts:
        df = MART_BUILDERS[name](spark, gold)
        df = add_gold_audit(df)
        df.cache()
        rows = df.count()
        write_delta(df, f"s3a://{config.gold_bucket}/{name}")
        results.append(MartResult(name=name, rows=rows))
        print(f"- {name}: {rows} linhas")

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Le o star schema da Gold e materializa data marts agregados "
            "(joins + agregacoes do modelo) em Delta Lake."
        ),
    )
    parser.add_argument(
        "--marts",
        nargs="*",
        default=[],
        help=(
            "Marts a (re)construir (separados por espaco ou virgula). Por padrao "
            "constroi todos. Conhecidos: " + ", ".join(ALL_MARTS) + "."
        ),
    )
    parser.add_argument(
        "--gold-bucket",
        help="Sobrescreve o bucket da Gold (padrao: MINIO_GOLD_BUCKET ou 'gold').",
    )
    return parser


def main() -> None:
    load_environment()

    args = build_parser().parse_args()
    config = AggConfig.from_env()

    if args.gold_bucket:
        config = AggConfig(gold_bucket=args.gold_bucket)

    selected = parse_names(args.marts) or list(ALL_MARTS)
    unknown = [m for m in selected if m not in ALL_MARTS]
    if unknown:
        raise SystemExit(
            f"Mart(s) desconhecido(s): {', '.join(unknown)}. "
            f"Conhecidos: {', '.join(ALL_MARTS)}."
        )

    selected = [m for m in ALL_MARTS if m in selected]

    print(f"Gold star schema: s3a://{config.gold_bucket}/<dim_ou_fato>/")
    print(f"Gold agregados  : s3a://{config.gold_bucket}/agg_<mart>/")
    print(f"Marts a construir: {len(selected)}\n")

    spark = build_spark_session(app_name="gold-agregados")
    try:
        results = run(spark, config, selected)
    finally:
        spark.stop()

    print(
        f"\nAgregacoes Gold concluidas. Marts gravados: {len(results)}."
    )


if __name__ == "__main__":
    main()
